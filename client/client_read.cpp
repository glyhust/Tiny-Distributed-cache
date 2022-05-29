#include "client_basical.hpp"

int main(int argc,char* argv[])
{

    Client cli;

    while(1)
    {
        string rkeyvalue=ReadText(KEY_FILE_NAME);
        int cut1=rkeyvalue.find_first_of(' ');
        int cut2=rkeyvalue.find_last_of(' ');

        string rkey=rkeyvalue.substr(0,cut1);
        string rvalue=rkeyvalue.substr(cut1+1,20);

        string value=cli.keyValueRequest(rkey);

        if(value==rvalue)
            cout<<"求取正确!"<<endl;
        else
            cout<<"求取错误!"<<endl;
    }

    return 0;

}




