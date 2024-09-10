#ifndef __LOGGER__
#define __LOGGER__

#include <iostream>
#include <string>
#include "constants.h"

using namespace std;

class Logger
{
public:
    static void log(string message, string value = "")
    {
        #ifdef LOG_ENABLE
        cout << message << value << endl;
        #endif
    }

    static void log(string message, double value)
    {
        #ifdef LOG_ENABLE
        cout << message << value << endl;
        #endif
    }

};
#endif
