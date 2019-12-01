#include <iostream>
#include <stdint.h>
#include <string>
#include <fstream> 
#include "kvdb.h" 
using namespace std;
using namespace kvdb;

struct KVData
{
	uint32_t  key_length;
	uint32_t  value_length;
	string key;
	string value;
};
//fstream file;
	
KVDBHandler::KVDBHandler(const std::string& db_file)
{
	assignpath_file=db_file;
	file.open(assignpath_file.c_str(),ios::in|ios::out|ios::app|ios::binary);
	if(file.is_open())
	{
		file.close();	
		file.open(assignpath_file.c_str(),ios::in|ios::out|ios::app|ios::binary);
		if(file.fail())
		{
			file.open(assignpath_file.c_str(),ios::in|ios::out|ios::app|ios::binary);
		}
	}
	else
	{
		//return KVDB_INVALID_AOF_PATH;
		file.open(assignpath_file.c_str(),ios::in|ios::out|ios::app|ios::binary);
	}
} 

string KVDBHandler::getpath_file()
{
	return this->assignpath_file;
}

fstream& KVDBHandler::getfile()
{
	return this->file;
} 

KVDBHandler::~KVDBHandler()
{
	file.close(); 
}


int kvdb::set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	fstream& tempfile = handler->getfile();
	uint32_t key_length = key.length();
	uint32_t value_length = value.length();
	
	tempfile.seekg(0,ios::end);
	tempfile.write(reinterpret_cast<char*>(&key_length),sizeof(uint32_t));
	tempfile.write(reinterpret_cast<char*>(&value_length),sizeof(uint32_t));
	const char *keykey = key.c_str();
	const char *valuevalue = value.c_str(); 
	tempfile << keykey;
	tempfile << valuevalue;
	
	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	fstream& tempfile = handler->getfile();
	
	// tempdata of KVData;
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length;
	char tempkey[1024];
	char tempvalue[1024];

	int existkey_flag = 0;
	string tempkey2;
	
	//base address is file header, offset is 0 , position in file header
	tempfile.seekg(0,ios::beg);
	while(tempfile.peek()!=EOF) 
	{
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		tempkey[tempkey_length]='\0';
		tempkey2 = tempkey;
		
		
		if(tempkey2 == key)
		{
			if(tempvalue_length == 0)
			{
				existkey_flag = 0;
				value.clear();
				continue;
			}
			else
			{
				value.resize(tempvalue_length);
				tempfile.read(reinterpret_cast<char*>(&value),tempvalue_length*sizeof(char));
				existkey_flag = 1;
			}	
		}
		else
		{
			tempfile.seekg(tempvalue_length,ios::cur);	
		}
		
	} 
	
	if(existkey_flag == 1) 
	return KVDB_OK;					//existence
	else
	return KVDB_INVALID_KEY;		//inexistence
}

int kvdb::del(KVDBHandler* handler, const std::string& key)
{
	fstream& tempfile = handler->getfile();
	
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length ;
	char tempkey[1024];
	
	int existkey_flag = 0;
	string tempkey2;
	
	tempfile.seekg(0,ios::beg);
	while(tempfile.peek()!=EOF) 
	{
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		tempkey[tempkey_length]='\0';
	    tempkey2 = tempkey;
	
		if(tempkey2 == key)
		{
			if(tempvalue_length == 0)
			{
				existkey_flag = 0;
				continue;
			}
			else
			{
				existkey_flag = 1;
			}
		}
		tempfile.seekg(tempvalue_length,ios::cur);	
	} 
	
	if(existkey_flag == 1) 
	{
		tempkey_length = key.length();
		tempvalue_length = 0;
		
		tempfile.seekg(0,ios::end);
		tempfile.write(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.write(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		const char *keykey = key.c_str();
		tempfile<<keykey;
		
		return KVDB_OK;
	}					
	else
	return KVDB_INVALID_KEY;
		
} 
