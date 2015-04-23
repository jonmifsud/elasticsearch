<?php

require_once EXTENSIONS . '/elasticsearch/vendor/autoload.php';

Class ElasticSearch
{

    /**
     * @var Elastica\Client
     */
    public static $client = null;

    /**
     * @var Elastica\Index
     */
    public static $index = null;

    /**
     * @var array
     */
    public static $types = array();

    /**
     * @var array
     */
    public static $mappings = array();

    /**
     * Creates the Elastica\Client instance using the passed credentials.
     *
     * @param string $host
     * @param string $index_name
     * @param string $username
     * @param string $password
     */
    public static function init($host='', $index_name='', $username='', $password='')
    {
        if(self::$client !== NULL && self::$index !== NULL) return;

        $config = Symphony::Engine()->Configuration()->get('elasticsearch');

        if(empty($host)) $host = $config['host'];
        if(empty($index_name)) $index_name = $config['index-name'];
        if(empty($username)) $username = $config['username'];
        if(empty($password)) $password = $config['password'];

        if(empty($host)) {
            throw new Exception('ElasticSearch "host" not set in configuration.');
        }

        if(empty($index_name)) {
            throw new Exception('ElasticSearch "index-name" not set in configuration.');
        }

        try {
            $client = new Elastica\Client(array('url' => $host));
            if(!empty($username) && !empty($password)) {
                $client->addHeader('Authorization', 'Basic ' . base64_encode($username . ':' . $password));
            }
            $client->getStatus();
        } catch (Exception $e) {
            throw new Exception('ElasticSearch client: ' . $e->getMessage());
        }

        $index = $client->getIndex($index_name);

        self::$client = $client;
        self::$index = $index;
    }

    /**
     * Returns the current Elastica Client
     *
     * @return Elastica\Client
     */
    public static function getClient()
    {
        return self::$client;
    }

    /**
     * Returns the current Elastica Index class
     *
     * @return Elastica\Index
     */
    public static function getIndex()
    {
        return self::$index;
    }

    /**
     * Resets this class instance by removing the Elastica\Client and emptying the class properties `$index`,
     * `$mappings` and `$types`
     */
    public static function flush()
    {
        self::$client = NULL;
        self::$index = NULL;
        self::$types = array();
        self::$mappings = array();
    }

    /**
     * Given the type handle (usually the section handle), this will return
     * the mapping meta data about this section in an associative array.
     *
     * @param string $handle
     * @return array
     */
    public static function getTypeByHandle($handle)
    {
        if(in_array($handle, self::$types)) {
            return self::$types[$handle];
        }

        self::$types = self::getAllTypes();
        return self::$types[$handle];
    }

    /**
     * This function determines which Sections in Symphony have a corresponding mapping in the
     * the ElasticSearch index. Sections must have a valid type defined in the
     * `WORKSPACE . /elasticsearch/` or it will be discarded. If a type is valid, but it's not
     * yet in ElasticSearch, it will also be omitted from the return.
     *
     * @return array
     *  An array of all indexes in ElasticSearch that correspond with Symphony section mappings
     */
    public static function getAllTypes()
    {
        self::init();

        if(count(self::$types) > 0) return self::$types;

        // Find all existing mappings in Elasticsearch
        $get_mappings = self::getIndex()->request('_mapping', Elastica\Request::GET, array());
        $all_mappings = $get_mappings->getData();
        self::$mappings = reset($all_mappings);

        // In some versions of ES, the mappings is actually nested two deep
        // with the top level being the index name (catered for above with reset())
        // and the next level being 'mappings'.
        if (isset(self::$mappings['mappings'])) {
            self::$mappings = self::$mappings['mappings'];
        }

        // Now find all possible mappings from the filesystem.
        $types = array();
        foreach(SectionManager::fetch() as $section) {

            $elasticsearch_mapping_file = sprintf('%s/elasticsearch/mappings/%s.json', WORKSPACE, Extension_Elasticsearch::createHandle($section->get('handle')));
            $symphony_mapping_file = sprintf('%s/elasticsearch/mappings/%s.php', WORKSPACE, Extension_Elasticsearch::createHandle($section->get('handle')));

            // no mapping, no valid type
            if(!file_exists($elasticsearch_mapping_file)) continue;

            require_once($symphony_mapping_file);
            $symphony_mapping_classname = sprintf('elasticsearch_%s', Extension_Elasticsearch::createHandle($section->get('handle')));
            $symphony_mapping_class = new $symphony_mapping_classname;

            $elasticsearch_mapping_json = file_get_contents($elasticsearch_mapping_file);
            $elasticsearch_mapping = json_decode($elasticsearch_mapping_json, FALSE);
            $mapped_fields = $elasticsearch_mapping->{$section->get('handle')}->properties;

            // invalid JSON
            if(!$mapped_fields) throw new Exception('Invalid mapping JSON for ' . $section->get('handle'));

            $fields = array();
            foreach($mapped_fields as $field => $mapping) $fields[] = $field;

            $type = self::getIndex()->getType($section->get('handle'));
            if(!isset(self::$mappings[$section->get('handle')])) $type = NULL;

            $types[$section->get('handle')] = (object)array(
                'section' => $section,
                'fields' => $fields,
                'type' => $type,
                'mapping_json' => $elasticsearch_mapping_json,
                'mapping_class' => $symphony_mapping_class
            );
        }

        self::$types = $types;
        return $types;
    }

    /**
     * Given a the section handle, this function will attempt to
     * create a corresponding mapping and type in the ElasticSearch index.
     *
     * @param string $section_handle
     * @return boolean
     */
    public static function createType($section_handle)
    {
        self::init();

        // Fetch the local mapping of this handle
        $local_type = self::getTypeByHandle($section_handle);
        $mapping = json_decode($local_type->mapping_json, TRUE);

        // Create a new Type and Type Mapping
        $type = new Elastica\Type(self::getIndex(), $section_handle);
        $type_mapping = new Elastica\Type\Mapping($type);
        foreach($mapping[$section_handle] as $key => $value) {
            $type_mapping->setParam($key, $value);
        }

        // Attempt to actually persist the mapping to ElasticSearch
        $response = $type->setMapping($type_mapping);
        if ($response->isOk()) {
            return self::getIndex()->refresh()->isOk();

        } else {
            throw new Exception(sprintf('Failure setting mapping for type %s. ElasticSearch error: %s',
                $section_handle,
                $response->getError()
            ));
        }
    }

    /**
     * Given an `$entry`, and optionally a `$section`, index this entry
     * in the appropriate index.
     *
     * @param Entry|integer $entry
     *  The entry object or the Entry ID
     * @param Section $entry
     * @return boolean
     */
    public static function indexEntry($entry, Section $section = null)
    {
        self::init();

        if(!$entry instanceOf Entry) {
            $entry = reset(EntryManager::fetch($entry));
        }

        if(!$section instanceOf Section) {
            $section = SectionManager::fetch($entry->get('section_id'));
        }

        $type = self::getTypeByHandle($section->get('handle'));
        if(!$type || !$type->type) return;

        // build an array of entry data indexed by field handles
        $data = array();

        foreach($section->fetchFields() as $f) {
            //if(!in_array($f->get('element_name'), $type->fields)) continue;
            $data[$f->get('element_name')] = $entry->getData($f->get('id'));
        }

        $data = $type->mapping_class->mapData($data, $entry);
        if($data) {
            $document = new Elastica\Document($entry->get('id'), $data);
            try {
                $doc = $type->type->addDocument($document);
            } catch(Exception $ex) {
                throw $ex;
            }
        } else {
            return self::deleteEntry($entry, $section);
        }

        return self::getIndex()->refresh()->isOk();
    }

    /**
     * Given the `$entry`, delete it from the index
     *
     * @param Entry|integer $entry
     * @param Section $section
     * @return boolean
     */
    public static function deleteEntry($entry, Section $section = null)
    {
        self::init();

        if(!$entry instanceOf Entry) {
            $entry = reset(EntryManager::fetch($entry));
        }

        if(!$section instanceOf Section) {
            $section = SectionManager::fetch($entry->get('section_id'));
        }

        $type = self::getTypeByHandle($section->get('handle'));
        if(!$type) return;

        try {
            $type->type->deleteById($entry->get('id'));
        } catch(Exception $ex) { }

        return self::getIndex()->refresh()->isOk();
    }

    /**
     * Given a keyword, filter it for nasties and other bits and pieces.
     * Inspired by Clinton Gormley's perl client
     *
     * @see https://github.com/clintongormley/ElasticSearch.pm/blob/master/lib/ElasticSearch/Util.pm
     * @link http://lucene.apache.org/core/old_versioned_docs/versions/3_0_0/queryparsersyntax.html
     * @param string $keywords
     * @return string
     */
    public static function filterKeywords($keywords)
    {
        // strip tags, should aid against XSS
        $keywords = strip_tags($keywords);
        // remove characters from start/end
        $keywords = trim($keywords, '-+ ');
        // append leading space for future matching
        $keywords = ' ' . $keywords;

        $keywords = \Elastica\Util::replaceBooleanWordsAndEscapeTerm($keywords);

        // remove fields such as `title:`
        $keywords = preg_replace("/([a-zA-Z0-9_-]+\:)/", "", $keywords);

        // remove multiple spaces
        $keywords = preg_replace("/\s{1,}/", " ", $keywords);
        // remove characters from start/end (again)
        $keywords = trim($keywords, '-+ ');
        // add trailing quotes if missing
        $quotes = substr_count($keywords, '"');
        if($quotes % 2) $keywords .= '"';

        return trim($keywords);
    }
}
