#pragma once

#include <Common/CurrentThread.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_nullable_dynamic_type;
}

inline bool allowNullableDynamicType()
{
    auto context = CurrentThread::getQueryContext();
    if (!context)
        context = Context::getGlobalContextInstance();
    if (!context)
        return false;

    return context->getSettingsRef()[Setting::allow_experimental_nullable_dynamic_type];
}

}
