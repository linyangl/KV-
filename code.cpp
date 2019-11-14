/*
 * Course Project for Data-Structure
 * Simple File Based K-V Database
 */
#include <iostream>
#include <exception>
#include <string>
#include <fstream>

// File Handler for KV-DB
class KVDBHandler 
{
	private:
		fstream hander;
		string assignfile_path;
    public:
        // Constructor, creates DB handler
        // @param db_file {const std::string&} path of the append-only file for database.
        KVDBHandler(const std::string& db_file);
		
        // Closes DB handler
        ~KVDBHandler();
}

    // Set the string value of a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key of a string
    // @param value {const std::string&} the value of a string
    // @return {int} return code
    	int set(KVDBHandler* handler, const std::string& key, const std::string& value);

    // Get the value of a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key of a string
    // @param value {std::string&} store the value of a key after GET executed correctly
    // @return {int} return code    
    	int get(KVDBHandler* handler, const std::string& key, std::string& value);

    // Delete a key
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key to be deleted
    // @return {int} return code
    	int del(KVDBHandler* handler, const std::string& key);

	// Purge the append-only file for database.
    // @param handler {KVDBHandler*} the handler of KVDB
    // @return {int} return code
    	int purge(KVDBHandler* handler);
    	
    // Set a key's time to live in seconds
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param n {int} life cycle in seconds
    // @return {int} return code
    	int expires(KVDBHandler* handler, const std::string& key, int n);

    // Set a member from a Set or SortedSet 's time to live in seconds
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param n {int} life cycle in seconds
    // @return {int} return code
    	int expires(KVDBHandler* handler, const std::string& key, const std::string& member, int n);
	
    // Remove and get a element from list head
    // @param handler {KVDBHandler*} the handler of KVDB
    // @param key {const std::string&} the key
    // @param value {std::string&} store the element when successful.
    // @return {int} return code
    

