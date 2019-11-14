#include<iostream>
#include<fstream> 
using namespace std;
int main()
{
	ofstream ofs;
	ofs.open("C:\\AMD\\a.txt",ios::out|ios::app|ios::binary);  
	ifstream ifs("C:\\AMD\\a.txt",ios::in|ios:: binary);
	string s="abcdefg",s1;
	ofs<<s;  //ofs.write()
	ifs>>s1;
	if(!ofs.fail())
	{
		cout<<s1<<endl;
	}
	else
	cout<<"error!"<<endl;
	ifs.close();
}
