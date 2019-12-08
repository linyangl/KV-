#include <iostream>
#include <stdint.h>
#include <string>
#include <unordered_map> 
#include <fstream> 
#include "kvdb2.h" 
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
	this->creatAOFindex();
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

int KVDBHandler::creatAOFindex()
{
	fstream &tempfile = this->getfile();
	tempfile.seekg(0,ios::beg);
	
	while(tempfile.peek()!=EOF)
	{
		int pos = tempfile.tellg();
		int tempkey_length;
		int tempvalue_length;
		char tempkey[1024];
		string tempkey2;
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		tempkey[tempkey_length]='\0';
	    tempkey2 = tempkey;
		if(tempvalue_length != 0)
		{
			this->AOF_Index[tempkey2] = pos;
		}
		else
			this->AOF_Index.erase(tempkey2);
		
		tempfile.seekg(tempvalue_length,ios::cur);
	}
	
//	unordered_map<string,int>::iterator it;
//	it = AOF_index.begin();
//	for(;it!=AOF_index.end();it++)
//	{
//		cout<<it->first<<":"<<it->second<<endl; 
//	}
	return KVDB_OK;
}

unordered_map<std::string,int>* KVDBHandler::getAOFIndex()
{
	return &this->AOF_Index;
}

int KVDBHandler::setOffset(const std::string &key,const int &pos)
{
	this->AOF_Index[key] = pos;
	return KVDB_OK;
}

int KVDBHandler::getOffset(const std::string &key)
{
	int pos;
	unordered_map<string,int>::iterator it = this->AOF_Index.find(key);
	if(it != this->AOF_Index.end())
	{
		pos=it->second;
		//cout<<it->first<<"::::"<<it->second<<endl; 
		return pos;
	}	
	return KVDB_NOT_EXISTS_KEY;
}

int KVDBHandler::deleteOffset(const std::string &key)
{
	this->AOF_Index.erase(key);
	return KVDB_OK;
}


int kvdb::set(KVDBHandler* handler, const std::string& key, const std::string& value)
{
	fstream& tempfile = handler->getfile();
	uint32_t key_length = key.length();
	uint32_t value_length = value.length();
	
	tempfile.seekg(0,ios::end);
	int pos = tempfile.tellg();
	tempfile.write(reinterpret_cast<char*>(&key_length),sizeof(uint32_t));
	tempfile.write(reinterpret_cast<char*>(&value_length),sizeof(uint32_t));
	const char *keykey = key.c_str();
	const char *valuevalue = value.c_str(); 
	tempfile << keykey;
	tempfile << valuevalue;
	handler->setOffset(keykey,pos);
	
	return KVDB_OK;
}

int kvdb::get(KVDBHandler* handler, const std::string& key, std::string& value)
{
	fstream& tempfile = handler->getfile();
	
	// tempdata of KVData;
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length;
	int pos;

	pos = handler->getOffset(key);
	if(pos == KVDB_NOT_EXISTS_KEY) 
		return KVDB_INVALID_KEY;
	
	tempfile.seekg(pos,ios::beg);
	tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
	tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
	tempfile.seekg(tempkey_length,ios::cur);
	value.resize(tempvalue_length);
	tempfile.read(reinterpret_cast<char*>(&value[0]),tempvalue_length*sizeof(char));	
	
	return KVDB_OK;					
}

int kvdb::del(KVDBHandler* handler, const std::string& key)
{
	fstream& tempfile = handler->getfile();
	
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length ;
	int pos;
	
	pos = handler->getOffset(key);
	if(pos == KVDB_NOT_EXISTS_KEY) return KVDB_INVALID_KEY;
	
	tempkey_length = key.length();
	tempvalue_length = 0;
		
	tempfile.seekg(0,ios::end);
	tempfile.write(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
	tempfile.write(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
	const char *keykey = key.c_str();
	tempfile<<keykey;
	handler->deleteOffset(key);	
	return KVDB_OK;		
} 

int kvdb::purge(KVDBHandler* handler)
{
	string newc_path = "new_dp_path";
	fstream ncfile(newc_path.c_str(),ios::in|ios::out|ios::app|ios::binary);

	fstream &tempfile = handler->getfile();
	unordered_map<string,int> *index = handler->getAOFIndex();
	unordered_map<string,int>::iterator it;
	it = index->begin();
	kvdb::KVDBHandler temphandler(newc_path);
	
	while(it != index->end())
	{
		string key = it->first;
		string value;
		
		if(get(handler,key,value) == KVDB_OK )
		{
			set(&temphandler,key,value);
		}
		it++;
	}
	
	handler->~KVDBHandler();
	temphandler.~KVDBHandler();
	string originalpath = handler->getpath_file(); 
	rename(originalpath.c_str(),newc_path.c_str());
	remove(originalpath.c_str());	
}

