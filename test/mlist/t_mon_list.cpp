#include <util/datastream/topk/fss/SpaceSaving.hpp>
#include <util/datastream/topk/cms/TopKCalculator.hpp>
#include <util/datastream/topk/cms/TopKItemEstimation.hpp>
#include <util/datastream/sketch/madoka/sketch.h>
#include <util/datastream/sketch/HyperLogLog.hpp>
#include <util/datastream/sketch/KMV.hpp>
#include <am/leveldb/Table.h>
#include <string>
#include <stdint.h>
#include <iostream>
#include <fstream>
#include <fstream>
#include <boost/lexical_cast.hpp>
#include <map>
#include <time.h>
#include <util/DynamicBloomFilter.h>
#include <log-server/LogAnalysisManager.h>
#include <log-server/StorageManager.cpp>
#include <log-server/LogServerCfg.cpp>

using namespace std;
using namespace sf1r::logserver;
#define MAXCOUNT 400

#define MAX_PRIME 2147483647
#define seed MAX_PRIME

#define TEXT_PROP 0
//test LogAnalysisManager
//write dvleveldb
#define TEXT_LAV  1
//test LogAnalysisManager
//without access to dvleveldb
#define TEXT_LA   0
//test KMV
#define TEXT_KMV  0
//test HyperLogLog
#define TEXT_HLL  0
//test Monitor list
#define TEXT_ML   0

typedef std::list< std::pair<std::string, uint32_t> > TopKT;
typedef std::list< std::map<std::string, std::string> > ValueT;

int main()
{
    if(TEXT_PROP)
    {
        LogAnalysisManager lam("log_server_storage/");
        std::string c = "b5mp";

        std::string prop;
        while(cin>>prop)
        {
            lam.insertPropertyLabel(c, prop, 1);
        }

        ValueT props;
        lam.getPropertyLabel(c, props);
        ValueT::iterator it;
        for(it = props.begin(); it!=props.end(); it++)
        {
            std::map<std::string, std::string>::iterator iter;
            for(iter=it->begin();iter!=it->end();iter++)
                cout <<"[ "<<iter->first<<", "<<iter->second<<" ] ";
            cout <<endl;
        }

    }
    if(TEXT_LAV)
    {
        LogAnalysisManager lam("log_server_storage/");
        std::string s = "user-query-analysis";
        std::string c = "b5mp";
        std::string l_str;

        while(cin >> l_str)
        {
            std::map<std::string, std::string> m;
            m["key"]=l_str;
            lam.insert(s, c, "20130509T000000", l_str, m);
//            lam.insert(s, c, l_str);
        }
        std::map<std::string, std::string> m2;
        m2["end"]="end";

        lam.insert(s, c, "20130510T100000", "end", m2);

        //get distinct value count
        uint64_t count = 0;
        lam.getDVC(s,c, "20130508T000000", "20130509T000000", count);
        cout <<"*****************************"<<endl;
        cout << "Distinct Value Count: "<<count << endl;
        cout <<"*****************************"<<endl;
        cout <<"TopK frequent items: "<<endl;

        for(int j=0;j<1;j++)
        {
            TopKT topk;
            lam.getTopK(s, c, "20130508T000000", "20130509T000000", 20, topk);
            TopKT::iterator iter;
            for(iter=topk.begin();iter!=topk.end();iter++)
                cout << iter->first << "    " << iter->second<<endl;
        }

        ValueT values;
        lam.getAllCollectionData(s, "20130508T000000", values);
        cout << "ok"<<endl;
        ValueT::iterator it;
        for(it=values.begin();it!=values.end();it++)
        {
            std::map<std::string, std::string>::iterator i;
            cout <<"item: ";
            for(i=it->begin();i!=it->end();i++)
                cout <<"["<<i->first<<", "<<i->second<<"]"<<" ";
            cout <<endl;
        }
    }
    if(TEXT_LA)
    {
        LogAnalysisManager lam("storage/");
        std::string s = "test";
        std::string c = "lam_str";
        std::string lam_str;
        while(cin >> lam_str)
        {
            lam.insert(s, c, lam_str);
        }
        uint64_t count = 0;
        lam.getDVC(s, c, count);
        cout << count << endl;

        TopKT topk;
        lam.getTopK(s, c, 400, topk);
        TopKT::iterator iter;
        for(iter=topk.begin();iter!=topk.end();iter++)
            cout << iter->first << "    " << iter->second<<endl;
    }
//test KMV
    if(TEXT_KMV)
    {
        typedef izenelib::util::KMV<uint128_t> KMVT;
        KMVT* kmv = new KMVT(0, 6);
        std::string kmv_str;

        std::map <std::string, int> kmv_map;
        while(cin >> kmv_str)
        {
            kmv_map[kmv_str] = 1;
            kmv->updateSketch(Utilities::md5ToUint128(kmv_str));
        }

        cout << "exact count: " << kmv_map.size() <<endl;
        cout << "distinct value count: " << kmv->getCardinate() << endl;

        ofstream kmv_fout("kmv.out");
        kmv->save(kmv_fout);

        kmv_fout.close();

        ifstream kmv_fin("kmv.out");
        delete kmv;
        kmv = new KMVT(0, 6);
        kmv->load(kmv_fin);
        cout << "distinct value count: "<<kmv->getCardinate() << endl;
    }
//test hll
    if(TEXT_HLL)
    {
        typedef izenelib::util::HyperLL<uint128_t> HyperLLT;
        HyperLLT* hll = new HyperLLT(seed, 6);
        std::string hll_str;

        std::map<std::string, int> hll1_map;
        while(cin >> hll_str)
        {
            hll1_map[hll_str] = 1;
            hll->updateSketch(Utilities::md5ToUint128(hll_str));
        }
        cout << "exact count: " << hll1_map.size() <<endl;
        cout <<"distinct value count: " << hll->getCardinate() << endl;

        //ofstream fout("hll.out");
        //hll->save(fout);

        //ifstream fin("hll.out");
        //delete hll;
        //hll = new HyperLLT(0, 8);
        //hll->load(fin);

        //cout << "distinct value count: "<<hll->getCardinate() << endl;
    }
//test monitored list
    if(TEXT_ML)
    {
        izenelib::util::MonitoredList<uint32_t, uint32_t, uint32_t> ml(MAXCOUNT);

        uint32_t ml_str;
        while(cin >> ml_str)
        {
            ml.insert(ml_str);
        }

        ml.show();
    }
    return 0;
}
