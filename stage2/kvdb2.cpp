#include <iostream>
#include <stdint.h>
#include <string>
#include <unordered_map> 
#include <fstream> 
#include <time.h>
#include <queue> 
#include "kvdb2.h" 
using namespace std;
using namespace kvdb;


Expiration_time::Expiration_time()
{
	key="";
	expiration_time=0;
}
bool Expiration_time::operator <(const Expiration_time &a) const
{
	return expiration_time>a.expiration_time;
}

Expiration_Index::Expiration_Index()
{
	offset=0;
	expiration_time=0;
}
	
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
		time_t new_time; 
		char tempkey[1024];
		string tempkey2;
		tempfile.read(reinterpret_cast<char*>(&tempkey_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempvalue_length),sizeof(uint32_t));
		tempfile.read(reinterpret_cast<char*>(&tempkey),tempkey_length*sizeof(char));
		tempkey[tempkey_length]='\0';
	    tempkey2 = tempkey;
		if(tempvalue_length == 0)
		{
			this->AOF_Index.erase(tempkey2);
		}
		else if(tempvalue_length ==-1)
		{
			tempfile.read(reinterpret_cast<char*>(&new_time), sizeof(time_t));
			setExpiration(tempkey2,new_time);
			Expiration_time exp;
			exp.key=tempkey2;
			exp.expiration_time=new_time;
			Time_queue.push(exp);
		}
		else
		{
				setOffset(tempkey2,pos); 
		}
		
		tempfile.seekg(tempvalue_length,ios::cur);
	}
	
	return KVDB_OK;
}

unordered_map<std::string,Expiration_Index>* KVDBHandler::getAOFIndex()
{
	return &this->AOF_Index;
}

int KVDBHandler::setOffset(const std::string &key,const int &pos)
{
	AOF_Index[key].offset= pos;
	AOF_Index[key].expiration_time = 0;
	return KVDB_OK;
}

int KVDBHandler::getOffset(const std::string &key,int &pos)
{
	//int pos;
	unordered_map<string,Expiration_Index>::iterator it = this->AOF_Index.find(key);
	if(it != this->AOF_Index.end())
	{
		pos=it->second.offset;
		return KVDB_OK;
	}	
	return -1;
}

int KVDBHandler::deleteOffset(const std::string &key)
{
	this->AOF_Index.erase(key);
	return KVDB_OK;
}


std::priority_queue<Expiration_time>* KVDBHandler::getTimequeue()
{
	return &Time_queue;
}

int KVDBHandler::getExpiration(const std::string &key,time_t &new_time)
{
	unordered_map<string,Expiration_Index>::iterator it=this->AOF_Index.find(key);
	if(it != this->AOF_Index.end())
	{
		new_time=it->second.expiration_time;
		return KVDB_OK;
	}
	return -1;
}

int KVDBHandler::setExpiration(const std::string &key,const time_t &new_time)
{
	this->AOF_Index[key].expiration_time = new_time;
	return KVDB_OK;
}

int KVDBHandler::updataKey()
{
	time_t t=time(NULL);
	time_t now = t;
	while(true)
	{
		if(Time_queue.empty())
		{
			break;
		}
		Expiration_time exp=Time_queue.top();
		if(exp.expiration_time>t)
		{
			break;
		}
		time_t new_time;
		getExpiration(exp.key,new_time);
		if(new_time == exp.expiration_time)
		{
			del(this,exp.key);
		}
		else
		{
			Time_queue.pop();
			continue;
		}
	 } 
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
	handler->updataKey();
	fstream& tempfile = handler->getfile();
	
	// tempdata of KVData;
	uint32_t  tempkey_length;
	uint32_t  tempvalue_length;
	int pos;

	int flag = handler->getOffset(key,pos);
	if(flag == -1) 
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
	
	int flag=handler->getOffset(key,pos);
	if(flag == -1) return KVDB_INVALID_KEY;
	
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
	unordered_map<string,Expiration_Index> *index = handler->getAOFIndex();
	unordered_map<string,Expiration_Index>::iterator it;
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

int kvdb::expires(KVDBHandler* handler, const std::string& key, int n)
{
	fstream &tempfile = handler->getfile();
	tempfile.seekg(0,ios::end);
	time_t t = time(NULL);
	int new_time=t+n;
	Expiration_time exp;
	exp.key=key;
	exp.expiration_time=new_time;
	std::priority_queue<Expiration_time>* q=handler->getTimequeue();
	q->push(exp);
	handler->setExpiration(key,new_time);
	int key_length=key.length(),value_length=-1;
	tempfile.write(reinterpret_cast<char *>(&key_length), sizeof(int)); 
	tempfile.write(reinterpret_cast<char *>(&value_length), sizeof(int)); 
	const char *keykey=key.c_str();  
	tempfile<<keykey;
	tempfile.write(reinterpret_cast<char *>(&new_time), sizeof(int)); 
	
}

