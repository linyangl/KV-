#include <iostream>
#include <stdint.h>
#include <string>
#include <fstream> 
#include "kvbasedata.h" 
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
namespace kvdb {
	
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


int set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	fstream& tempfile = handler->getfile();
	
	KVData data;
	data.key_length = key.length();
	data.value_length = value.length();
	data.key = key;
	data.value = value;
	
	tempfile.seekg(0,ios::end);
	
	tempfile.write(reinterpret_cast<char*>(&data.key_length),sizeof(uint32_t));
	tempfile.write(reinterpret_cast<char*>(&data.value_length),sizeof(uint32_t));
	//tempfile.write(reinterpret_cast<char*>(&data.key),data.key_length*sizeof(unsigned int));
	//tempfile.write(reinterpret_cast<char*>(&data.value),data.value_length*sizeof(unsigned int));
	tempfile<<data.key.c_str();
	tempfile<<data.value.c_str();
	return KVDB_OK;
}

int get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	fstream& tempfile = handler->getfile();
	
	// tempdata of KVData;
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length;
	string tempkey;
	string tempvalue;
	value.clear();
	
	int existkey_flag = 0;
	
	//base address is file header, offset is 0 , position in file header
	tempfile.seekg(0,ios::beg);
	while(tempfile.peek()!=EOF) 
	{
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		
		if(tempvalue_length == 0)
		{
			tempfile.seekg(tempkey_length,ios::cur);
			continue;
		}
		
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		if(tempkey == key)
		{
			tempfile.read(reinterpret_cast<char*>(&tempvalue),tempvalue_length*sizeof(char));
			tempvalue[tempvalue_length]='\0';
			value = tempvalue;
			existkey_flag = 1;
		}
		tempfile.seekg(tempvalue_length,ios::cur);	
		
	} 
	if(existkey_flag == 1) 
	return KVDB_OK;					//existence
	else
	return KVDB_NOT_EXISTS_KEY;		//inexistence
}

int del(KVDBHandler* handler, const std::string& key)
{
	fstream& tempfile = handler->getfile();
	KVData tempdata; 
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length ;
	string tempkey;
	string tempvalue;
	
	int existkey_flag = 0;
	
	//base address is file header, offset is 0 , position in file header
	tempfile.seekg(0,ios::beg);
	while(tempfile.peek()!=EOF) 
	{
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		
		if(tempvalue_length == 0)
		{
			tempfile.seekg(tempkey_length,ios::cur);
			existkey_flag = 0;
			continue;
		}
		
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		if(tempkey == key)
		{
			//tempfile.read(reinterpret_cast<char*>(&tempvalue),tempvalue_length*sizeof(unsigned int));
			tempvalue_length = 0;
			existkey_flag = 1;
		}
		tempfile.seekg(tempvalue_length,ios::cur);	
	} 
	
	if(existkey_flag == 1) 
	{
		//uint32_t  key_length;
		//uint32_t  value_length;
		//string key;
		//string value;
		tempdata.key_length =  tempkey_length;
		tempdata.value_length = tempvalue_length;
		tempdata.key = tempkey;
		
		tempfile.seekg(0,ios::end);
		tempfile.write(reinterpret_cast<char*>(&tempdata.key_length),sizeof(uint32_t));
		tempfile.write(reinterpret_cast<char*>(&tempdata.value_length),sizeof(uint32_t));
//		tempfile.write(reinterpret_cast<char*>(&tempdata.key),tempdata.key_length*sizeof(char));
//		tempfile.write(reinterpret_cast<char*>(&tempdata.value),tempdata.value_length*sizeof(char));
		tempfile<<tempdata.key.c_str();
		return KVDB_OK;
	}					
	else
	return KVDB_NOT_EXISTS_KEY;
		
}
} 
