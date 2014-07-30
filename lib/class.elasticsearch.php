<?php

// function __autoload_elastica ($class) {
// 	$path = str_replace('\\', '/', $class);
// 	$load = EXTENSIONS . '/elasticsearch/lib/Elastica/lib/' . $path . '.php';
// 	if (file_exists($load)) require_once($load);
// }
// spl_autoload_register('__autoload_elastica');

require 'vendor/autoload.php';

// function __autoload_elasticsearch ($class) {
// 	$path = str_replace('\\', '/', $class);
// 	$load = EXTENSIONS . '/elasticsearch/lib/' . $path . '.php';
// 	if (file_exists($load)) require_once($load);
// }
// spl_autoload_register('__autoload_elasticsearch');

require_once(TOOLKIT . '/class.sectionmanager.php');

Class ElasticSearch {
	
	public static $client = null;
	public static $index = null;
	public static $types = array();
	public static $mappings = array();
	
	public static function init($host='', $index_name='', $username='', $password='') {
		
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
			$params = array(
				'hosts' => array($host),
				// 'index' => $index_name
			);
			if(!empty($username) && !empty($password)) {
				$params['connectionParams']['auth'] = array(
				    $username,
				    $password,
				    'Basic' 
				);
			}
			$client = new Elasticsearch\Client($params);
			$client->ping();
		} catch (Exception $e) {
			throw new Exception('ElasticSearch client: ' . $e->getMessage());
		}
		
		// $index = $client->indices()->get($index_name);
		$index = $index_name;

		$indexParams['index']  = $index_name;
		//$auto_create_index && 
		if(!$client->indices()->exists($indexParams)) {			
			// $indexParams['body']['settings']['number_of_shards']   = 3;
			// $indexParams['body']['settings']['number_of_replicas'] = 2;
			$index = $client->indices()->create($indexParams);
		}
		

		self::$client = $client;
		self::$index = $index;
	}
	
	public static function flush() {
		self::$client = NULL;
		self::$index = NULL;
		self::$types = array();
		self::$mappings = array();
	}
	
	public static function getIndex() {
		return self::$index;
	}
	
	public static function getClient() {
		return self::$client;
	}
	
	public static function getTypeByHandle($handle) {
		if(in_array($handle, self::$types)) {
			return self::$types[$handle];
		}
		
		self::$types = self::getAllTypes();
		return self::$types[$handle];
	}
	
	public static function getAllTypes() {
		self::init();
		
		if(count(self::$types) > 0) return self::$types;
		
		$sm = new SectionManager(Symphony::Engine());
		
		$get_mappings = self::$client->indices()->getMapping(array('index'=>self::$index));
		// var_dump($get_mappings);die;
		// $all_mappings = $get_mappings[$index]; //= $get_mappings->getData();
		self::$mappings = $get_mappings[self::$index]['mappings'];//reset($all_mappings);
		
		$types = array();
		foreach($sm->fetch() as $section) {
			
			$elasticsearch_mapping_file = sprintf('%s/elasticsearch/mappings/%s.json', WORKSPACE, preg_replace('/-/', '_', $section->get('handle')));
			$symphony_mapping_file = sprintf('%s/elasticsearch/mappings/%s.php', WORKSPACE, preg_replace('/-/', '_', $section->get('handle')));
			
			// no mapping, no valid type
			if(!file_exists($elasticsearch_mapping_file)) continue;
			
			require_once($symphony_mapping_file);
			$symphony_mapping_classname = sprintf('elasticsearch_%s', preg_replace('/-/', '_', $section->get('handle')));
			$symphony_mapping_class = new $symphony_mapping_classname;
			
			$elasticsearch_mapping_json = file_get_contents($elasticsearch_mapping_file);
			$elasticsearch_mapping = json_decode($elasticsearch_mapping_json, FALSE);
			$mapped_fields = $elasticsearch_mapping->{$section->get('handle')}->properties;
			
			// invalid JSON
			if(!$mapped_fields) throw new Exception('Invalid mapping JSON for ' . $section->get('handle'));
			
			$fields = array();
			foreach($mapped_fields as $field => $mapping) $fields[] = $field;
			
			// var_dump(self::$mappings);die;

			// $type = self::getIndex()->getType($section->get('handle'));
			$type = $section->get('handle');
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
	
	public static function createType($handle) {
		self::init();
		
		$local_type = self::getTypeByHandle($handle);
		$mapping = json_decode($local_type->mapping_json, TRUE);

		var_dump($mapping);
		
		$type = new Elastica\Type(self::getIndex(), $handle);

		echo('<br/><br/>');
		var_dump($type);
		
		$type_mapping = new Elastica\Type\Mapping($type);
		foreach($mapping[$handle] as $key => $value) {
			$type_mapping->setParam($key, $value);
		}

		echo('<br/><br/>');
		var_dump($type_mapping);
		echo('<br/><br/>');
		
		var_dump( $type->setMapping($type_mapping) );//die;
		self::$client->indices()->refresh(array('index'=>self::$index));
	}
	
	public static function indexEntry($entry, $section=NULL) {
		self::init();
		
		if(!$entry instanceOf Entry) {
			// build the entry
			$em = new EntryManager(Symphony::Engine());
			$entry = reset($em->fetch($entry));
		}
		
		if(!$section instanceOf Section) {
			// build section
			$sm = new SectionManager(Symphony::Engine());
			$section = $sm->fetch($entry->get('section_id'));
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
			$params = array(
				'index' => self::$index,
				'type' => $section->get('handle'), //$type,
				'id' => $entry->get('id'),
				'body' => $data
			);
			self::$client->index($params);
		} else {
			self::deleteEntry($entry, $section);
		}
		
		self::$client->indices()->refresh(array('index'=>self::$index));
		
	}
	
	public static function deleteEntry($entry, $section=NULL) {
		
		if(!$entry instanceOf Entry) {
			// build the entry
			$em = new EntryManager(Symphony::Engine());
			$entry = reset($em->fetch($entry));
		}
		
		if(!$section instanceOf Section) {
			// build section
			$sm = new SectionManager(Symphony::Engine());
			$section = $sm->fetch($entry->get('section_id'));
		}
		
		$type = self::getTypeByHandle($section->get('handle'));
		if(!$type) return;

		try {
			$type->type->deleteById($entry->get('id'));
		} catch(Exception $ex) { }
		
		self::$client->indices()->refresh(array('index'=>self::$index));
	}
	
	/* 
	Inspired by Clinton Gormley's perl client
		https://github.com/clintongormley/ElasticSearch.pm/blob/master/lib/ElasticSearch/Util.pm
	Full list of query syntax
		http://lucene.apache.org/core/old_versioned_docs/versions/3_0_0/queryparsersyntax.html
	*/
	public static function filterKeywords($keywords) {
		// strip tags, should aid against XSS
		$keywords = strip_tags($keywords);
		// remove characters from start/end
		$keywords = trim($keywords, '-+ ');
		// append leading space for future matching
		$keywords = ' ' . $keywords;
		// remove wilcard `*` and `?` and fuzzy `~`
		$keywords = preg_replace("/\*|\?|\~/", "", $keywords);
		// remove range syntax `{}`
		$keywords = preg_replace("/\{|\}/", "", $keywords);
		// remove group `()` and`[]` chars
		$keywords = preg_replace("/\(|\)|\[|\]/", "", $keywords);
		// remove boost `^`
		$keywords = preg_replace("/\^/", "", $keywords);
		// remove not `!`
		$keywords = preg_replace("/\!/", "", $keywords);
		// remove and `&&`
		$keywords = preg_replace("/\&\&/", "", $keywords);
		// remove or `||`
		$keywords = preg_replace("/\|\|/", "", $keywords);
		// remove fields such as `title:`
		$keywords = preg_replace("/([a-zA-Z0-9_-]+\:)/", "", $keywords);
		// remove `-` that don't have spaces before them
		$keywords = preg_replace("/(?<! )-/", "", $keywords);
		// remove the spaces after a + or -
		$keywords = preg_replace("/([+-])\s+/", "", $keywords);
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