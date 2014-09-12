/**
 * @file process/common/OnSignal.cpp
 * @author Ian Yang
 * @date Created <2009-10-29 13:19:47>
 * @date Updated <2010-08-19 10:16:59>
 */
#include "OnSignal.h"

#include <boost/bind.hpp>
#include <boost/bind/apply.hpp>

#include <algorithm>
#include <vector>
#include <iostream>

#include <cstdlib>
// only register signal for system that supports
# include <signal.h>

#ifndef HAVE_SIGNAL_H
#define HAVE_SIGNAL_H
#endif

namespace sf1r
{

void setupDefaultSignalHandlers()
{
#ifdef HAVE_SIGNAL_H
    registerExitSignal(SIGINT);
    registerExitSignal(SIGQUIT);
    registerExitSignal(SIGTERM);
    registerExitSignal(SIGHUP);
#endif // HAVE_SIGNAL_H
}

namespace   // {anonymous}
{

/**
 * @brief get global OnExit in current compilation unit
 */
std::vector<OnSignalHook>& gOnExit()
{
    static std::vector<OnSignalHook> onExit;
    return onExit;
}

void gForceExit(int)
{
    _exit(1);
}

void gRunHooksOnExitWithSignal(int signal)
{
    try
    {
        std::for_each(
            gOnExit().begin(),
            gOnExit().end(),
            boost::bind(boost::apply<void>(), _1, signal)
        );
    }
    catch (...)
    {
        std::cerr << "Uncaught Exception on exit." << std::endl;
        _exit(1);
    }
}
} // namespace {anonymous}

void gRunHooksOnExit()
{
    gRunHooksOnExitWithSignal(0);
}

void addExitHook(const OnSignalHook& hook)
{
    gOnExit().push_back(hook);
}

void registerExitSignal(int signal)
{
    if (signal == 0)
    {
        atexit(gRunHooksOnExit);
        return;
    }

    struct ::sigaction action;
    action.sa_handler = gRunHooksOnExitWithSignal;
    ::sigemptyset(&action.sa_mask);
    action.sa_flags = 0;

    sigaction(signal, &action, 0);
}

void gThrowException(int)
{
    throw std::runtime_error("bad malloc" );
}

void registerExceptionSignal(int signal)
{
    struct ::sigaction action;
    action.sa_handler = gThrowException;
    ::sigemptyset(&action.sa_mask);
    action.sa_flags = 0;

    sigaction(signal, &action, 0);
}

} // namespace sf1r
