<?php


/**
 * Class for handling metadata files in serialized format.
 *
 * @package simpleSAMLphp
 */
class SimpleSAML_Metadata_MetaDataStorageHandlerSerialize extends SimpleSAML_Metadata_MetaDataStorageSource {

    /**
     * Database handle as a  SimpleSAML_Store_SQL object
     */
	private $dbh = NULL;
    private $tablename;


	/**
	 * Constructor for this metadata handler.
	 *
	 * Parses configuration.
	 *
	 * @param array $config  The configuration for this metadata handler.
	 */
	public function __construct($config) {
		assert('is_array($config)');

        $this->dbh = new SimpleSAML_Store_SQL();
        $this->tablename = $this->dbh->prefix . "_metadatastore";

	}


	/**
	 * Retrieve a list of all available metadata sets.
	 *
	 * @return array  An array with the available sets.
	 */
	public function getMetadataSets() {

		$ret = NULL;
        $resultset = $this->dbh->pdo->query("SELECT DISTINCT _set FROM ". $this->tablename);
        $ret = $resultset->fetchAll(PDO::FETCH_COLUMN, 0);

		if ($ret === FALSE) {
			SimpleSAML_Logger::warning(__CLASS__.' ' . var_export($resultset->errorInfo(), TRUE));
			return $ret;
		}

		return $ret;
	}


	/**
	 * Retrieve a list of all available metadata for a given set.
	 *
	 * @param string $set  The set we are looking for metadata in.
	 * @return array  An associative array with all the metadata for the given set.
	 */
	public function getMetadataSet($set) {
		assert('is_string($set)');

		$ret = array();

        $query = $this->dbh->pdo->prepare("SELECT DISTINCT _entity, _value FROM ". $this->tablename. " WHERE _set = :set");
        $params = array('set' => $set );
        if( FALSE === $query->execute($params)){
			SimpleSAML_Logger::warning(__CLASS__.' '.__LINE__.' Failed to get metadata: ' . var_export($query->errorInfo(), TRUE));
        
        }
        
    
        foreach( $resultset as $row ){
            $ret[$row['_entity']] = unserialize($row['_value']);
            if( FALSE === $ret[$row['_entity']]){
			    SimpleSAML_Logger::warning('Error deserializing metadata: '.$row['_entity']);
            }
        }
		return $ret;
	}


	/**
	 * Retrieve a metadata entry.
	 *
	 * @param string $entityId  The entityId we are looking up.
	 * @param string $set  The set we are looking for metadata in.
	 * @return array  An associative array with metadata for the given entity, or NULL if we are unable to
	 *         locate the entity.
	 */
	public function getMetaData($entityId, $set) {
		assert('is_string($entityId)');
		assert('is_string($set)');

        $query = $this->dbh->pdo->prepare("SELECT _value FROM ". $this->tablename. " WHERE _set = :set AND _entity = :entity");
        $params = array('set' => $set, 'entity' => $entityId );
        if( FALSE === $query->execute($params)){
			SimpleSAML_Logger::warning(__CLASS__.' '.__LINE__.' Failed to get metadata: ' . var_export($query->errorInfo(), TRUE));
            return NULL;
        }
        
        $result = $query->fetchAll(PDO::FETCH_COLUMN, 0);
        if( FALSE === $result){
			SimpleSAML_Logger::warning(__CLASS__.' '.__LINE__.' Failed to get metadata: ' . var_export($query->errorInfo(), TRUE));
            return NULL;
        }
        if( 1 != count($result)){ 
            // Metadata not found which is not error
            // or the (metadata,set) is ambiguous, which contradicts database constraint
            return NULL;  
        }
        $data = unserialize($result[0]);
        if( FALSE === $ret){
            return NULL;
        }
    
		return $data;
	}


	/**
	 * Save a metadata entry.
	 *
	 * @param string $entityId  The entityId of the metadata entry.
	 * @param string $set  The metadata set this metadata entry belongs to.
	 * @param array $metadata  The metadata.
	 */
	public function saveMetadata($entityId, $set, $metadata) {
		assert('is_string($entityId)');
		assert('is_string($set)');
		assert('is_array($metadata)');

        $keys = array('_set', '_entity');
        $values = array(
            '_set' => $set,
            '_entity' => $entityId,
            '_value' => serialize($metadata),
            );
        try{
            $dbh->insertOrUpdate($this->tablename, $keys, $values);
		}catch(PDOException $e) {
            SimpleSAML_Logger::error('Failed to save metadata.');
            return FALSE;
        }
        return TRUE;
	}


	/**
	 * Delete a metadata entry.
	 *
	 * @param string $entityId  The entityId of the metadata entry.
	 * @param string $set  The metadata set this metadata entry belongs to.
	 */
	public function deleteMetadata($entityId, $set) {
		assert('is_string($entityId)');
		assert('is_string($set)');

	    $query = $this->dbh->pdo->prepare("DELETE FROM ". $this->tablename. " WHERE _set = :set AND _entity = :entity");
        $params = array('set' => $set, 'entity' => $entityId );
        if( FALSE === $query->execute($params)){
			SimpleSAML_Logger::error('Failed to delete file metadata '. $entityId .' in set '.$set);
        }
 
	}


    /**
     * Clean expired metadatas from the database 
     *
     */
    public function cleanMetadataTable() {

        $sets = $this->getMetadataSets();
        
        foreach( $sets as $set){
            $metadatas = getMetaData($set);   
            foreach( $metadatas as $entity => $data){
                if($data['expire'] < time()){
                    $this->deleteMetadata($entity, $set);
                }
            }
        }
    
    }


}
?>
