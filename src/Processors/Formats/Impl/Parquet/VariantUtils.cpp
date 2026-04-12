#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/NestedUtils.h>
#include <IO/ReadHelpers.h>

namespace DB::Parquet
{

namespace
{

bool tryInsertFlatJSONPath(Object & object, std::string_view path, Field value)
{
    auto [head, tail] = Nested::splitName(path);
    String key = unescapeDotInJSONKey(String(head));

    if (tail.empty())
    {
        auto [_, inserted] = object.try_emplace(std::move(key), std::move(value));
        return inserted;
    }

    auto [it, inserted] = object.try_emplace(key, Object{});
    if (!inserted && it->second.getType() != Field::Types::Object)
        return false;

    return tryInsertFlatJSONPath(it->second.safeGet<Object>(), tail, std::move(value));
}

}

bool isNullableStringType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeString *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return typeid_cast<const DataTypeString *>(nullable->getNestedType().get());

    return false;
}

DataTypePtr unwrapVariantTypeHint(DataTypePtr type)
{
    while (type)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable->getNestedType();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality->getDictionaryType();
            continue;
        }

        break;
    }

    return type;
}

DataTypePtr makeVariantExactOutputTypeNullable(const DataTypePtr & type)
{
    if (!type)
        return type;

    if (typeid_cast<const DataTypeNullable *>(type.get()))
        return type;

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        DataTypePtr dictionary_type = low_cardinality->getDictionaryType();
        if (!typeid_cast<const DataTypeNullable *>(dictionary_type.get()))
            dictionary_type = makeNullable(dictionary_type);
        return std::make_shared<DataTypeLowCardinality>(dictionary_type);
    }

    return makeNullable(type);
}

String appendVariantJSONPath(std::string_view parent_path, std::string_view key)
{
    String escaped_key = escapeDotInJSONKey(String(key));
    if (parent_path.empty())
        return escaped_key;

    String result;
    result.reserve(parent_path.size() + 1 + escaped_key.size());
    result.append(parent_path.data(), parent_path.size());
    result.push_back('.');
    result += escaped_key;
    return result;
}

bool tryBuildNestedObjectField(Field flat_object_field, Field & nested_object_field)
{
    if (flat_object_field.getType() != Field::Types::Object)
        return false;

    Object nested_object;
    Object flat_object = std::move(flat_object_field).safeGet<Object>();
    for (auto & [path, value] : flat_object)
    {
        if (!tryInsertFlatJSONPath(nested_object, path, std::move(value)))
            return false;
    }

    nested_object_field = std::move(nested_object);
    return true;
}

std::optional<Field> tryNestVariantJSONPaths(Field field)
{
    switch (field.getType())
    {
        case Field::Types::Object:
        {
            Field nested_object;
            if (!tryBuildNestedObjectField(std::move(field), nested_object))
                return std::nullopt;

            Object object = std::move(nested_object).safeGet<Object>();
            for (auto & [_, value] : object)
            {
                auto nested_value = tryNestVariantJSONPaths(std::move(value));
                if (nested_value.has_value())
                    value = std::move(*nested_value);
            }

            return Field(std::move(object));
        }
        case Field::Types::Array:
        {
            Array array = std::move(field).safeGet<Array>();
            for (auto & element : array)
            {
                auto nested_element = tryNestVariantJSONPaths(std::move(element));
                if (nested_element.has_value())
                    element = std::move(*nested_element);
            }
            return Field(std::move(array));
        }
        case Field::Types::Tuple:
        {
            Tuple tuple = std::move(field).safeGet<Tuple>();
            for (auto & element : tuple)
            {
                auto nested_element = tryNestVariantJSONPaths(std::move(element));
                if (nested_element.has_value())
                    element = std::move(*nested_element);
            }
            return Field(std::move(tuple));
        }
        case Field::Types::Map:
        {
            Map map = std::move(field).safeGet<Map>();
            for (auto & entry_field : map)
            {
                if (entry_field.getType() != Field::Types::Tuple)
                    continue;

                Tuple entry = std::move(entry_field).safeGet<Tuple>();
                if (entry.size() != 2)
                    continue;

                auto nested_value = tryNestVariantJSONPaths(std::move(entry[1]));
                if (nested_value.has_value())
                    entry[1] = std::move(*nested_value);

                entry_field = std::move(entry);
            }
            return Field(std::move(map));
        }
        default:
            return field;
    }
}

std::optional<VariantWrapperLayout> tryGetVariantWrapperLayout(const DataTypePtr & type)
{
    const IDataType * nested_type = type.get();
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(nested_type))
        nested_type = nullable->getNestedType().get();

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_type);
    if (!tuple_type || !tuple_type->hasExplicitNames())
        return std::nullopt;

    const size_t num_elements = tuple_type->getElements().size();
    if (num_elements == 0 || num_elements > 2)
        return std::nullopt;

    auto value_pos = tuple_type->tryGetPositionByName("value");
    if (!value_pos.has_value() || !isNullableStringType(tuple_type->getElement(*value_pos).get()))
        return std::nullopt;

    std::optional<size_t> typed_value_pos;
    DataTypePtr typed_value_type;
    if (num_elements == 2)
    {
        typed_value_pos = tuple_type->tryGetPositionByName("typed_value");
        if (!typed_value_pos.has_value())
            return std::nullopt;
        typed_value_type = tuple_type->getElement(*typed_value_pos);
    }
    else if (*value_pos != 0)
    {
        return std::nullopt;
    }

    return VariantWrapperLayout
    {
        .value_pos = *value_pos,
        .typed_value_pos = typed_value_pos,
        .typed_value_type = std::move(typed_value_type),
    };
}

bool isDirectDynamicCompatibleVariantTypedType(const DataTypePtr & type)
{
    if (!type)
        return false;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        return isDirectDynamicCompatibleVariantTypedType(nullable->getNestedType());

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return isDirectDynamicCompatibleVariantTypedType(low_cardinality->getDictionaryType());

    if (auto wrapper = tryGetVariantWrapperLayout(type))
    {
        if (!wrapper->typed_value_pos.has_value())
            return true;
        return isDirectDynamicCompatibleVariantTypedType(wrapper->typed_value_type);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & element : tuple_type->getElements())
        {
            if (!isDirectDynamicCompatibleVariantTypedType(element))
                return false;
        }

        return true;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return isDirectDynamicCompatibleVariantTypedType(array_type->getNestedType());

    if (type->getName() == "Bool")
        return true;

    switch (type->getTypeId())
    {
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::String:
        case TypeIndex::FixedString:
            return true;
        default:
            return false;
    }
}

const DataTypeObject * tryGetObjectLikeVariantOutputType(const IDataType * type)
{
    while (type)
    {
        if (const auto * object = typeid_cast<const DataTypeObject *>(type))
            return object;

        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        {
            type = nullable->getNestedType().get();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type))
        {
            type = low_cardinality->getDictionaryType().get();
            continue;
        }

        break;
    }

    return nullptr;
}

bool isObjectLikeVariantOutputType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeObject *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return isObjectLikeVariantOutputType(nullable->getNestedType().get());

    return false;
}

bool isDynamicLikeVariantOutputType(const IDataType * type)
{
    if (!type)
        return false;

    if (typeid_cast<const DataTypeDynamic *>(type))
        return true;

    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type))
        return isDynamicLikeVariantOutputType(nullable->getNestedType().get());

    return false;
}

bool isComplexVariantExactOutputType(const DataTypePtr & type)
{
    const IDataType * normalized = type.get();
    while (normalized)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(normalized))
        {
            normalized = nullable->getNestedType().get();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(normalized))
        {
            normalized = low_cardinality->getDictionaryType().get();
            continue;
        }

        break;
    }

    if (!normalized)
        return false;

    return typeid_cast<const DataTypeArray *>(normalized)
        || typeid_cast<const DataTypeTuple *>(normalized)
        || typeid_cast<const DataTypeMap *>(normalized)
        || typeid_cast<const DataTypeObject *>(normalized)
        || typeid_cast<const DataTypeDynamic *>(normalized)
        || typeid_cast<const DataTypeVariant *>(normalized);
}

}
