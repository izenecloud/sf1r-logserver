#include "ProcessOptions.h"

#include <boost/lexical_cast.hpp>

#include <iostream>

using namespace std;
using namespace boost;

namespace po = boost::program_options;

void validate(boost::any& v,
              const std::vector<std::string>& values,
              ProcessOptions::Port*, int)
{
    static const unsigned kPortLowerBound = 1024;
    static const std::string kPortLowerBoundStr = "1024";

    using namespace boost::program_options;
    validators::check_first_occurrence(v);
    const string& value = validators::get_single_string(values);

    unsigned port = 0;
    try
    {
        port = boost::lexical_cast<unsigned>(value);
    }
    catch (boost::bad_lexical_cast&)
    {
        throw invalid_option_value("Port requires an unsigned integer.");
    }

    if (port <= kPortLowerBound)
    {
        throw invalid_option_value("Port must be higher than " + kPortLowerBoundStr);
    }

    v = boost::any(ProcessOptions::Port(port));
}

void validate(boost::any& v,
              const std::vector<std::string>& values,
              ProcessOptions::String*, int)
{
    using namespace boost::program_options;
    validators::check_first_occurrence(v);
    const string& value = validators::get_single_string(values);

    if (value.empty() || value[0] == '-')
    {
        throw invalid_option_value(
            "Option value cannot start with hyphen: " + value
        );
    }

    v = boost::any(ProcessOptions::String(value));
}

ProcessOptions::ProcessOptions()
    : cobraProcessDescription_("Cobra Process Options")
    , logServerProcessDescription_("Log Server Process Options")
    , colorServerProcessDescription_("Color Server Process Options")
    , isVerboseOn_(false)
    , logPrefix_("")
{
    additional_.add("additional_", 0);
    po::options_description base;
    po::options_description configDir;
    po::options_description configFile;
    po::options_description scdParsing;
    po::options_description verbose;
    po::options_description logPrefix;
    po::options_description pidFile;

    logPrefix.add_options()
    ("log-prefix,l", po::value<String>(),
     "Prefix including log Path or one of pre-defined values.\n\n"
     "Pre-defined values:\n"
     "  stdout: \tDisplay log message into stdout\n"
     "  NULL: \tNo logging\n\n"
     "Example: -l ./log/BA\n"
     "  path: \tlog\n"
     "  prefix: \tBA\n"
     "  log file: \t./BA.info.xxx ./BA.error.xxx ...\n"
    );

    pidFile.add_options()
    ("pid-file", po::value<String>(),
     "Specify the file to store pid");

    verbose.add_options()
    ("verbose,v", "verbosely display log information");

    base.add_options()
    ("help,H", "Display help message");

    configDir.add_options()
    ("config-directory,F", po::value<String>(), "Path to the directory contained configuration files");

    configFile.add_options()
    ("config-file,F", po::value<String>(), "Path to the configuration file");

    cobraProcessDescription_.add(base).add(verbose).add(logPrefix).add(configDir).add(pidFile);

    logServerProcessDescription_.add(base).add(verbose).add(configFile).add(pidFile);
    colorServerProcessDescription_.add(base).add(verbose).add(configFile).add(pidFile);
}


void ProcessOptions::setProcessOptions()
{
    if (variableMap_.count("config-directory")) //-F in BA
    {
        configFileDir_ = variableMap_["config-directory"].as<String>().str;
    }

    if (variableMap_.count("config-file"))
    {
        configFile_ = variableMap_["config-file"].as<String>().str;
    }

    if (variableMap_.count("verbose"))
    {
        isVerboseOn_ = true;
    }
    if (variableMap_.count("log-prefix"))
    {
        logPrefix_ = variableMap_["log-prefix"].as<String>().str;
    }

    pidFile_.clear();
    if (variableMap_.count("pid-file"))
    {
        pidFile_ = variableMap_["pid-file"].as<String>().str;
    }
}

bool ProcessOptions::setCobraProcessArgs(const std::vector<std::string>& args)
{
    std::string cobraProcessSample = "Example: ./CobraProcess -F ./config";
    try
    {
        po::store(po::command_line_parser(args).options(cobraProcessDescription_).positional(additional_).run(), variableMap_);
        po::notify(variableMap_);

        bool flag = variableMap_.count("verbose");
        unsigned int size = variableMap_.size();
        if (variableMap_.count("help") || variableMap_.empty())
        {
            cout << "Usage:  CobraProcess <settings (-F)[, -V]>" << endl;
            cout << cobraProcessSample<<endl;
            cout << cobraProcessDescription_;
            return false;
        }
        if (size < 1 || (flag && size < 2))
        {
            cerr << "Warning: Missing Mandatory Parameter" << endl;
            cout << "Usage:  CobraProcess <settings (-F)[, -V]>" << endl;
            cout << cobraProcessSample<<endl;
            cout << cobraProcessDescription_;
            return false;
        }
        setProcessOptions();
    }
    catch (std::exception &e)
    {
        cerr << "Warning: " << e.what() << std::endl;
        cout << "Usage:  CobraProcess <settings (-F )[, -V]>" << endl;
        cout << cobraProcessSample << endl;
        cout << cobraProcessDescription_;
        return false;
    }
    return true;
}

bool ProcessOptions::setLogServerProcessArgs(const std::string& processName, const std::vector<std::string>& args)
{
    try
    {
        po::store(po::command_line_parser(args).options(logServerProcessDescription_).positional(additional_).run(), variableMap_);
        po::notify(variableMap_);

        if (!variableMap_.count("help") && variableMap_.count("config-file"))
        {
            setProcessOptions();
            return true;
        }
    }
    catch (std::exception& e)
    {
        cerr << "Caught exception: " << e.what() << endl;
    }

    // Print usage
    cout << "Usage:   " << processName << " <settings (-F )[, -H, ...]>" << endl;
    cout << "Exapmle: " << processName << " -F ./config/logserver.cfg" << endl;
    cout << logServerProcessDescription_ << endl;

    return false;
}

bool ProcessOptions::setImageServerProcessArgs(const std::string& processName, const std::vector<std::string>& args)
{
    try
    {
        po::store(po::command_line_parser(args).options(colorServerProcessDescription_).positional(additional_).run(), variableMap_);
        po::notify(variableMap_);

        if (!variableMap_.count("help") && variableMap_.count("config-file"))
        {
            setProcessOptions();
            return true;
        }
    }
    catch (std::exception& e)
    {
        cerr << "Caught exception: " << e.what() << endl;
    }

    // Print usage
    cout << "Usage:   " << processName << " <settings (-F )[, -H, ...]>" << endl;
    cout << "Exapmle: " << processName << " -F ./config/colorserver.cfg" << endl;
    cout << colorServerProcessDescription_ << endl;

    return false;
}
