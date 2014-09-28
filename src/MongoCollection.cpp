#include "ext_mongo.h"
#include "bson_decode.h"
#include "contrib/encode.h"

namespace HPHP {

    static ObjectData *
    create_object(const StaticString * className, Array params) {
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

    static mongoc_collection_t *get_collection(Object obj) {
        mongoc_collection_t *collection;

        auto db = obj->o_realProp("db", ObjectData::RealPropUnchecked, "MongoCollection")->toObject();
        auto client = db->o_realProp("client", ObjectData::RealPropUnchecked, "MongoDB")->toObject();
        String db_name = db->o_realProp("db_name", ObjectData::RealPropUnchecked, "MongoDB")->toString();
        String collection_name = obj->o_realProp("name", ObjectData::RealPropUnchecked, "MongoCollection")->toString();

        collection = mongoc_client_get_collection(get_client(client)->get(), db_name.c_str(), collection_name.c_str());
        return collection;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // class MongoCollection

    /**
     * Inserts a document into the collection
     *
     * @param array|object $a - a    An array or object. If an object is
     *   used, it may not have protected or private properties.    If the
     *   parameter does not have an _id key or property, a new MongoId
     *   instance will be created and assigned to it.
     * @param array $options - options    Options for the insert.
     *
     * @return bool|array - Returns an array containing the status of the
     *   insertion if the "w" option is set. Otherwise, returns TRUE if the
     *   inserted array is not empty (a MongoException will be thrown if the
     *   inserted array is empty). 
     */
    //public function insert(mixed $a, array $options = array()): mixed;

    static Variant HHVM_METHOD(MongoCollection, insert, Variant a, Array options) {
        mongoc_collection_t *collection;
        bson_t doc;
        bson_error_t error;

        collection = get_collection(this_);

        Array& doc_array = a.toArrRef();
        if (!doc_array.exists(String("_id"))) {
            const StaticString s_MongoId("MongoId");
            char id[25];
            bson_oid_t oid;
            bson_oid_init(&oid, NULL);
            bson_oid_to_string(&oid, id);
            ObjectData * data = create_object(&s_MongoId, make_packed_array(String(id)));
            doc_array.add(String("_id"), data);
        }
        encodeToBSON(doc_array, &doc);




        int w_flag = MONGOC_WRITE_CONCERN_W_DEFAULT;
        //如果传递了参数
        
        mongoc_write_concern_t *write_concern;
        write_concern = mongoc_write_concern_new();
        mongoc_write_concern_set_w(write_concern, w_flag);
        
        bool ret = mongoc_collection_insert(collection, MONGOC_INSERT_NONE, &doc, write_concern, &error);
        if (!ret) {
            mongoThrow<MongoCursorException>((const char *) error.message);
        }
        mongoc_collection_destroy(collection);
        bson_destroy(&doc);
        return ret;
        /*
        bool mongoc_collection_insert (mongoc_collection_t           *collection,
                                      mongoc_insert_flags_t          flags,
                                      const bson_t                  *document,
                                      const mongoc_write_concern_t  *write_concern,
                                      bson_error_t                  *error);
         */

    }


    /**
     * Remove records from this collection
     *
     * @param array $criteria - criteria    Description of records to
     *   remove.
     * @param array $options - options    Options for remove.    "justOne"
     *    Remove at most one record matching this criteria.
     *
     * @return bool|array - Returns an array containing the status of the
     *   removal if the "w" option is set. Otherwise, returns TRUE.
     */
    //public function remove(array $criteria = array(), array $options = array()): mixed;

    static Variant HHVM_METHOD(MongoCollection, remove, Array criteria, Array options) {
        mongoc_collection_t *collection;
        bson_t criteria_b;
        bson_error_t error;

        collection = get_collection(this_);

        encodeToBSON(criteria, &criteria_b);
        mongoc_delete_flags_t delete_flag = MONGOC_DELETE_NONE;
        int w_flag = MONGOC_WRITE_CONCERN_W_DEFAULT;
        //如果传递了参数
        if(!options.empty()){
            //printf("multiple = %s\r\n",options[String("multiple")].toBoolean() ? "true":"false");
            if(options[String("justOne")].toBoolean()==true){
                delete_flag = MONGOC_DELETE_SINGLE_REMOVE;
            }
            
        }
        mongoc_write_concern_t *write_concern;
        write_concern = mongoc_write_concern_new();
        mongoc_write_concern_set_w(write_concern, w_flag);
        bool ret = mongoc_collection_delete(collection, delete_flag, &criteria_b, write_concern, &error);

        if (!ret) {
            mongoThrow<MongoCursorException>((const char *) error.message);
        }
        mongoc_collection_destroy(collection);
        bson_destroy(&criteria_b);
        
        
        
        return ret;
        /*
        bool mongoc_collection_delete (mongoc_collection_t           *collection,
                                      mongoc_delete_flags_t          flags,
                                      const bson_t                  *selector,
                                      const mongoc_write_concern_t  *write_concern,
                                      bson_error_t                  *error);
         */
    }

    /*
    public function update(array $criteria,
                             array $new_object,
                             array $options = array()): mixed;
     */
    static Variant HHVM_METHOD(MongoCollection, update, Array criteria, Array new_object, Array options) {
        mongoc_collection_t *collection;
        bson_t selector; //selector is the criteria (which document to update)
        bson_t update; //update is the new_object containing the new data 
        const bson_t * collection_error; //update is the new_object containing the new data 
        bson_error_t error;
        collection = get_collection(this_);

        encodeToBSON(criteria, &selector);
        encodeToBSON(new_object, &update);
        
        //先定义一些默认的参数
        mongoc_update_flags_t update_flag = MONGOC_UPDATE_NONE;
        //todo
        int w_flag = MONGOC_WRITE_CONCERN_W_DEFAULT;
        
        
        //如果传递了参数
        if(!options.empty()){
            //printf("multiple = %s\r\n",options[String("multiple")].toBoolean() ? "true":"false");
            if(options[String("multiple")].toBoolean()==true){
                update_flag = MONGOC_UPDATE_MULTI_UPDATE;
            }
            if(options[String("upsert")].toBoolean()==true){
                update_flag = MONGOC_UPDATE_UPSERT;
            }
        }
        mongoc_write_concern_t *write_concern;
        write_concern = mongoc_write_concern_new();
        mongoc_write_concern_set_w(write_concern, w_flag);
   
        bool ret = mongoc_collection_update(collection, update_flag, &selector, &update, write_concern, &error);
        bson_destroy(&update);
        bson_destroy(&selector);
        if (!ret) {
            mongoThrow<MongoCursorException>((const char *) error.message);
        }
        collection_error = mongoc_collection_get_last_error(collection);
        //char *str;
        //if (collection_error) {
            //for debug
            //str = bson_as_json(collection_error, NULL);
            //printf("debug = %s\r\n",str);
            //bson_free(str);
        //}
        Array collectionReturn = Array();
        Array ouput = Array();
        collectionReturn = cbson_loads(collection_error);
        mongoc_collection_destroy(collection);
        ouput.add(String("ok"),1);
        ouput.add(String("nModified"),collectionReturn[String("nModified")]);
        ouput.add(String("n"),collectionReturn[String("nMatched")]);
        ouput.add(String("updatedExisting"),collectionReturn[String("nMatched")].toInt64() > 0 ?true:false);//todo 
        
        ouput.add(String("err"),collectionReturn[String("writeErrors")]);
        ouput.add(String("errmsg"),collectionReturn[String("writeErrors")]);
        
        const StaticString s_MongoTimestamp("MongoTimestamp");
        Array mongotimestampParam = Array();
        ObjectData * data = create_object(&s_MongoTimestamp,mongotimestampParam);
        ouput.add(String("lastOp"), data);
        ouput.add(String("mongoRaw"),collectionReturn);
        
        
        return ouput;
        //return ret;

        
    }


    ////////////////////////////////////////////////////////////////////////////////

    void MongoExtension::_initMongoCollectionClass() {
        HHVM_ME(MongoCollection, insert);
        HHVM_ME(MongoCollection, remove);
        HHVM_ME(MongoCollection, update);
    }

} // namespace HPHP
