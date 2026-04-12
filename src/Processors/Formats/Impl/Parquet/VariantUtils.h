#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

#include <optional>

namespace DB
{
class DataTypeObject;
}

namespace DB::Parquet
{

bool isNullableStringType(const IDataType * type);

DataTypePtr unwrapVariantTypeHint(DataTypePtr type);
DataTypePtr makeVariantExactOutputTypeNullable(const DataTypePtr & type);

String appendVariantJSONPath(std::string_view parent_path, std::string_view key);
bool tryBuildNestedObjectField(Field flat_object_field, Field & nested_object_field);
std::optional<Field> tryNestVariantJSONPaths(Field field);

struct VariantWrapperLayout
{
    size_t value_pos = 0;
    std::optional<size_t> typed_value_pos;
    DataTypePtr typed_value_type;
};

std::optional<VariantWrapperLayout> tryGetVariantWrapperLayout(const DataTypePtr & type);
bool isDirectDynamicCompatibleVariantTypedType(const DataTypePtr & type);

const DataTypeObject * tryGetObjectLikeVariantOutputType(const IDataType * type);
bool isObjectLikeVariantOutputType(const IDataType * type);
bool isDynamicLikeVariantOutputType(const IDataType * type);
bool isComplexVariantExactOutputType(const DataTypePtr & type);

}
