#include <bson.h>
#include "./contrib/classes.h"
#include "hphp/runtime/base/base-includes.h"
#include "bson_decode.h"
#include "ext_mongo.h"

namespace HPHP {

// Helper Function to Instantiate Defined Mongo Classes
// Adapted from HNI example
static ObjectData * 
create_object(const StaticString * className, Array params)
{
  TypedValue ret;
  Class * cls = Unit::loadClass(className -> get());
  ObjectData * obj = ObjectData::newInstance(cls);
  obj->incRefCount();

  g_context->invokeFunc(
    &ret,
    cls->getCtor(),
    params,
    obj
  );
  return obj;
}

static bool
cbson_loads_visit_double (const bson_iter_t *iter,
                          const char        *key,
                          double             v_double,
                          void              *output)
{
  ((Array *) output)->add(String(key),v_double);
  return false;
}

static bool
cbson_loads_visit_utf8 (const bson_iter_t *iter,
                        const char        *key,
                        size_t             v_utf8_len,
                        const char        *v_utf8,
                        void              *output)
{
  ((Array *) output)->add(String(key),v_utf8); 
  return false;
}

// Pre-declaration so compiler doesn't complain about
// undefined methods
static bool
cbson_loads_visit_document (const bson_iter_t *iter,
                            const char        *key,
                            const bson_t      *v_document,
                            void              *data);


static bool
cbson_loads_visit_array (const bson_iter_t *iter,
                         const char        *key,
                         const bson_t      *v_array,
                         void              *data);

static bool
cbson_loads_visit_binary (const bson_iter_t *iter,
                          const char        *key,
                          bson_subtype_t     v_subtype,
                          size_t             v_binary_len,
                          const uint8_t     *v_binary,
                          void              *output)
{
  ObjectData * data = create_object(&s_MongoBinData,
    make_packed_array(
      String((const char*) v_binary, v_binary_len, CopyString),
      (int) v_subtype));
  ((Array *) output) -> add(String(key), data);
  return false;

}                       

static bool
cbson_loads_visit_oid (const bson_iter_t *iter,
                       const char        *key,
                       const bson_oid_t  *oid,
                       void              *output)
{
  char id[25];
  bson_oid_to_string(oid, id);
  ObjectData * data = create_object(&s_MongoId, 
    make_packed_array(String(id)));
  ((Array *) output)->add(String(key), data);
  return false;
}

static bool
cbson_loads_visit_bool (const bson_iter_t *iter,
                        const char        *key,
                        bool              v_bool,
                        void              *output)
{
  ((Array *) output)->add(String(key), v_bool);
  return false;
}

static bool
cbson_loads_visit_date_time (const bson_iter_t *iter,
                             const char        *key,
                             int64_t       msec_since_epoch,
                             void              *output)
{
  // Renaming for convenience
  int64_t msec = msec_since_epoch;

  ObjectData * data = create_object(&s_MongoDate,
    make_packed_array(msec / 1000, (msec % 1000) * 1000));

  ((Array *) output)->add(String(key), data);

  return false;
} 

static bool
cbson_loads_visit_null (const bson_iter_t *iter,
                        const char        *key,                          
                        void              *output)
{
  ((Array *) output)->add(String(key), Variant());
  return false;
}

static bool
cbson_loads_visit_regex (const bson_iter_t *iter,
                         const char        *key,
                         const char        *regex,
                         const char        *options,
                         void              *output)
{
  String regex_string = "/" + String(regex) + "/" + String(options);
  ObjectData * data = create_object(&s_MongoRegex,
    make_packed_array(regex_string));

  ((Array *) output)->add(String(key), data); 

  return false;
}

static bool
cbson_loads_visit_dbpointer (const bson_iter_t *iter,
                             const char        *key,
                             size_t             v_collection_len,
                             const char        *v_collection,
                             const bson_oid_t  *v_oid,
                             void              *output)
{
  char id[25];
  bson_oid_to_string(v_oid, id);
  ObjectData * data = create_object(&s_MongoDBRef,
    make_packed_array(
      String(v_collection, v_collection_len, CopyString),
      String(id)));
  ((Array *)output)->add(String(key), data);
  return false;
  //TODO: Finish this
}

static bool
cbson_loads_visit_code (const bson_iter_t *iter,
                        const char        *key,
                        size_t             v_code_len,
                        const char        *v_code,
                        void              *output)
{
  ObjectData * data = create_object(&s_MongoCode,
    make_packed_array(String(v_code, v_code_len, CopyString)));
  ((Array *)output)->add(String(key), data);
  return false;
}

static bool
cbson_loads_visit_int32 (const bson_iter_t *iter,
                         const char         *key,
                         int32_t           v_int32,
                         void             *output)
{
  ((Array *) output)->add(String(key), v_int32);
  return false;
}

// Not implemented in cbson.c
static bool
cbson_loads_visit_timestamp (const bson_iter_t *iter,
                             const char        *key,
                             uint32_t          timestamp,
                             uint32_t          increment,
                             void              *output)
{
  ObjectData * data = create_object(&s_MongoTimestamp,
    make_packed_array((int64_t)timestamp, (int64_t)increment));

  ((Array *) output)->add(String(key), data);

  return false;
}

static bool
cbson_loads_visit_int64 (const bson_iter_t *iter,
                         const char*        key,
                         int64_t          v_int64,
                         void              *output)
{
  ((Array *) output)->add(String(key),v_int64);  
  return false;
}

static bool
cbson_loads_visit_maxkey (const bson_iter_t *iter,
                          const char*        key,
                          void              *output)
{
  ObjectData * data = create_object(&s_MongoMaxKey, Array());
  ((Array *)output)->add(String(key), data);
  return false;
}

static bool
cbson_loads_visit_minkey (const bson_iter_t *iter,
                          const char*        key,
                          void              *output)
{
  ObjectData * data = create_object(&s_MongoMinKey, Array());
  ((Array *)output)->add(String(key), data);
  return false;
}

static const 
bson_visitor_t gLoadsVisitors = {
  NULL, //TODO: visit before
  NULL, //TODO: visit after
  NULL, //TODO: visit corrupt
  cbson_loads_visit_double,
  cbson_loads_visit_utf8,
  cbson_loads_visit_document,
  cbson_loads_visit_array,
  cbson_loads_visit_binary, //TODO: visit binary
  NULL, //TODO: visit undefined
  cbson_loads_visit_oid,
  cbson_loads_visit_bool,
  cbson_loads_visit_date_time,  
  cbson_loads_visit_null,
  cbson_loads_visit_regex,
  cbson_loads_visit_dbpointer, //TODO: visit dbpointer
  cbson_loads_visit_code, //TODO: visit code
  NULL, //TODO: visit symbol
  NULL, //TODO: visit code with scope
  cbson_loads_visit_int32,  
  cbson_loads_visit_timestamp,
  cbson_loads_visit_int64,
  cbson_loads_visit_maxkey,
  cbson_loads_visit_minkey,
};

static bool
cbson_loads_visit_document (const bson_iter_t *iter,
                            const char        *key,
                            const bson_t      *v_document,
                            void              *data)
{
  bson_iter_t child;
  Array * ret = (Array *) data;
  Array obj;

  bson_return_val_if_fail(iter, true);
  bson_return_val_if_fail(key, true);
  bson_return_val_if_fail(v_document, true);

  if (bson_iter_init(&child, v_document))
  {
    obj = Array();
    if (!bson_iter_visit_all(&child, &gLoadsVisitors, &obj))
    {
      ret->add(String(key), obj);
    }
  }
  return false;
}

static bool
cbson_loads_visit_array (const bson_iter_t *iter,
                         const char        *key,
                         const bson_t      *v_array,
                         void              *data)
{
  bson_iter_t child;
  Array * ret = (Array *) data;
  Array obj;

  bson_return_val_if_fail(iter, true);
  bson_return_val_if_fail(key, true);
  bson_return_val_if_fail(v_array, true);

  if (bson_iter_init(&child, v_array))
  {
    obj = Array();
    if (!bson_iter_visit_all(&child, &gLoadsVisitors, &obj))
    {
      ret->add(String(key), obj);
    }
  }
  return false;
}

Array
cbson_loads (const bson_t * bson) 
{
  bson_iter_t iter;

  Array ret = Array();

  if (!bson_iter_init(&iter, bson))
  {
    mongoThrow<MongoException>("Failed to initialize BSON iterator");
  }
  bson_iter_visit_all(&iter, &gLoadsVisitors, &ret);

  return ret;
}



Array
cbson_loads_from_string (const String& bson) 
{
  bson_reader_t * reader;
  const bson_t * obj;
  bool reached_eof;

  Array output = Array();

  reader = bson_reader_new_from_data((uint8_t *)bson.c_str(), bson.size());
  
  if (!(obj = bson_reader_read(reader, &reached_eof))) {
    mongoThrow<MongoException>("Unexpected end of BSON. Input document is likely corrupted!");
  }  

  output = cbson_loads(obj);
  bson_reader_destroy(reader);

  return output;
}
// Namespace
}