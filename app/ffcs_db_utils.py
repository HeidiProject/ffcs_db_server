from pymongo import MongoClient
import pymongo
import datetime
from dateutil import parser
from DbCollections import DbCollections
import DbDataSchema
import bson
import copy


################################
# NOTE: If changes are made to location or port of database or database name, they also needs to be incorporated
# and deployed in other software that uses them, like ZMQ server/clients deployed in Docker containers
# Implemented Pydantic Settings for the ENV variables

class Settings:
    pass

def load_env_variables(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                value = value.strip('"').strip("'")
                setattr(Settings, key, value)

load_env_variables('.env')

######################
# TODO
# 1. decorator to that collecion.find always gets back a list
# 2. expose db object to general access to the database
# 3. logger

def send_notification(notification_type):
    #TODO add camapign_id to notifications
    """Decorator to mark that method needs to send notification.
       => collection_name is a name of collection when insert operation is performed

       The wrapped function has to have following syntax:
        def some_function(dictionary_parameter, other_params)
            where dictionary_parameter has following keys {'userAccount':user_account, 'campaignId':campaign_id,
                                                            'other_key':val....}

       or
        def some_function(user_account, campaign_id, other args)
            where 'user_account' and campaign_id are 1st and 2nd arguments

       """
    def real_decorator(db_operation):
        def wrapper(*args, **kwargs):
            wrapped = db_operation(*args, **kwargs)  # calls decorated function
            try:
                test = wrapped.inserted_id != ''
            except AttributeError:
                try:
                    test = wrapped['nModified'] == 1
                except AttributeError:
                    raise Exception('Sending notification implemented only for insert and update operations')
            if test:  # send notification only when insert or update was successful
                self = args[0]  # enables methods of ffcsdbclient
                if isinstance(args[1], dict):
                    user_account = args[1]['userAccount']
                    campaign_id = args[1]['campaignId']
                else:
                    user_account = args[1]
                    campaign_id = args[2]
                self.send_notification(user_account, campaign_id, notification_type)
            return wrapped
        return wrapper
    return real_decorator

class LibraryAlreadyImported(Exception):
    pass

def print_update_result(result):
    print("Matched documents: ", result.matched_count)
    print("Modified documents: ", result.modified_count)
    print("Upserted Id: ", result.upserted_id)
    print("Raw result: ", result.raw_result)

class ffcs_db_utils(object):
    def __init__(self, database_uri=Settings.URI):
        ### MongoDB on Atlas ### 
        self._client = MongoClient(database_uri, serverSelectionTimeoutMS=5000)
        self._db = self._client[Settings.DATABASE_NAME]

    ### FETCH_TAG delete_by_id
    def delete_by_id(self, collection_name, doc_id):
        """
        -- Delete a document in the specified collection using its ObjectId.
        Added for the purpose of deleting documents created in the unittest integration test (Alexander Metz)
        """
        collection = self.__get_collection(collection_name)
        result = collection.delete_one({"_id": bson.ObjectId(doc_id)})
        return result.acknowledged
    ### FETCH_TAG delete_by_id

    ### FETCH_TAG delete_by_query
    def delete_by_query(self, collection_name: str, query: dict):
        """
        -- Delete documents in the specified collection that match the provided query.
        Added for the purpose of deleting documents created in the unittest integration test (Alexander Metz)
        """
        collection = self.__get_collection(collection_name)
        result = collection.delete_many(query)
        return result.acknowledged
    ### FETCH_TAG delete_by_query

    ### FETCH_TAG get_collection
    def __get_collection(self, name):
        collection_name = getattr(DbCollections(), name)
        collection = self._db[collection_name]
        return collection
    ### FETCH_TAG get_collection

    ### FETCH_TAG merge_two_dictionaries
    def __merge_two_dictionaries(self, d1, d2):
        """
        Function added for python 2.* backwards compatybility.
           In Python3 merging two dicts can be done with {**d1, **d2}
        """
        out = copy.deepcopy(d1)
        out.update(d2)

        return out
    ### FETCH_TAG merge_two_dictionaries

    ### FETCH_TAG update_by_object_id
    # @send_notification('wells') # blocks gui in long running loops. call refresh_all_content() after update instead
    def update_by_object_id(self, user, campaign_id, collection, doc_id, **kwargs):
        """Update document in the given collection. The document to update if found by doc_id, which is ObjectId.
           The parameters to update are given in kwargs, example:
           update_by_object_id('wells', ObejctId('some'), **{'field1':val1, 'field2':val2}
        """
        collection = self.__get_collection(collection.lower())
        if not isinstance(doc_id, bson.objectid.ObjectId):
            raise RuntimeError('ffcsdbclient - doc_id must be ObjectId object')
        query = {'userAccount': user, 'campaignId': campaign_id, '_id':doc_id}
        update = {'$set': kwargs}
        r = collection.update_one(query, update, False)
        
        return r
    ### FETCH_TAG update_by_object_id

    ### FETCH_TAG update_by_object_id_NEW
    # @send_notification('wells') # blocks gui in long running loops. call refresh_all_content() after update instead
    def update_by_object_id_NEW(self, user, campaign_id, collection, doc_id, **kwargs):
        """
        Update document in the given collection. The document to update if found by doc_id, which is ObjectId.
        The parameters to update are given in kwargs, example:
        update_by_object_id('wells', ObejctId('some'), **{'field1':val1, 'field2':val2}
        """
        collection = self.__get_collection(collection.lower())
        if not isinstance(doc_id, bson.objectid.ObjectId):
            raise RuntimeError('ffcsdbclient - doc_id must be ObjectId object')
        query = {'userAccount': user, 'campaignId': campaign_id, '_id':doc_id}
        update = {'$set': kwargs}
        r = collection.update_one(query, update, False)
        
        # Mimic the old update result structure
        old_result_format = {'nModified': r.modified_count, 
                             'ok': 1.0 if r.acknowledged else 0.0, 
                             'n': r.matched_count}
        
        return old_result_format
    ### FETCH_TAG update_by_object_id_NEW

    ### FETCH_TAG add_plate
    @send_notification('plates')
    def add_plate(self, plate):
        collection = self.__get_collection('plates')
        collection_name = DbCollections().plates

        try:
            r = collection.insert_one(plate)
        except pymongo.errors.WriteError as e:
            raise RuntimeError('FFCS_DB write error: Document failed validation for collection {}. '
                               'One of the required elements is missing or has wrong type. '
                               'For correct schema refer to: '
                               'https://git.psi.ch/mx/ffcs/tree/master/ffcs_db.\n'
                               'You tried to insert:\n {}'.format(collection_name, plate))
        return r
    ### FETCH_TAG add_plate

    ### FETCH_TAG get_plates
    def get_plates(self, user_account, campaign_id):
        query = {'userAccount': user_account, 'campaignId': campaign_id}
        collection = self.__get_collection('plates')
        r = collection.find(query)
        return r
    ### FETCH_TAG get_plates

    ### FETCH_TAG get_plate
    def get_plate(self, user_account, campaign_id, plate_id):
        query = {'userAccount': user_account, 'plateId': plate_id, 'campaignId': campaign_id}
        collection = self.__get_collection('plates')
        r = collection.find_one(query)
        return r
    ### FETCH_TAG get_plate

    ### FETCH_TAG is_plate_in_database
    def is_plate_in_database(self, plate_id):
        """
        Given the plate_id of plate, checks if plate exists in database
        :param plate_id:
        :return: True/False
        """
        query = {'plateId': plate_id}
        collection = self.__get_collection('plates')
        r = collection.find_one(query)
        if r is None:
            return False
        return True
    ### FETCH_TAG is_plate_in_database

    ### FETCH_TAG get_unselected_plates
    def get_unselected_plates(self, user_account):
        """Returns places for which soaking places has not been selected
           -> return format:
              [{'_id': ObjectId('5ad743f3a03b252edbeb464e'),
              'batchId': None,
              'campaignId': 'kuba',
              'createdOn': datetime.datetime(2018, 4, 18, 15, 11, 15, 918000),
              'dropVolume': 125.0,
              'lastImaged': None,
              'plateId': 1111,
              'plateType': 'SwissCl',
              'soakExportTime': datetime.datetime(2018, 4, 18, 15, 38, 55, 268000),
              'soakPlacesSelected': False,
              'userAccount': 'e15880'}]
        """
        query = {'userAccount': user_account, 'soakPlacesSelected': False}
        collection = self.__get_collection('plates')
        r = collection.find(query)
        rlist = list(r)
        return rlist
    ### FETCH_TAG get_unselected_plates

    ### FETCH_TAG mark_plate_done
    @send_notification('plates')
    def mark_plate_done(self, user_account, campaign_id, plate_id, last_imaged, batch_id):
        query = {'userAccount': user_account, 'campaignId': campaign_id, 'plateId': plate_id}
        update = {'$set': {'soakPlacesSelected': True, 'last_imaged': last_imaged, 'batchId': batch_id}}
        collection = self.__get_collection('plates')
        result = collection.update_one(query, update, upsert=False)
        
        # Mimic the old update result structure
        old_result_format = {'nModified': result.modified_count, 
                             'ok': 1.0 if result.acknowledged else 0.0, 
                             'n': result.matched_count,
                             'updatedExisting': result.modified_count > 0}
        
        return old_result_format
    ### FETCH_TAG mark_plate_done

    ###
    ### Group of methods for campaigns
    ###

    ### FETCH_TAG get_campaigns
    def get_campaigns(self, user_account):
        """
        Find all the campaigns belonging to the user
        :param user_account:
        :return:
        """
        query = {'userAccount': user_account}
        collection = self.__get_collection('plates')
        # r = collection.find_one(query)
        r = collection.find(query)
        campaigns = r.distinct('campaignId')
        return campaigns
    ### FETCH_TAG get_campaigns

    # Group of methods for Wells
    ### FETCH_TAG add_wells
    def add_wells(self, list_of_wells):
        user = None
        campaign_id = None
        for incoming_well in list_of_wells:
            user = incoming_well['userAccount']
            campaign_id = incoming_well['campaignId']
            well = DbDataSchema.WellDataSchema(incoming_well['userAccount'], incoming_well['campaignId'],
                                               incoming_well['plateId'], incoming_well['well'],
                                               incoming_well['wellEcho'], incoming_well['x'], incoming_well['y'],
                                               incoming_well['xEcho'], incoming_well['yEcho'])
            self.add_well(well)
        ### time.sleep(0.1)

        # Send notification only if at least one well was inserted to database
        if user is not None and campaign_id is not None:
            self.send_notification(user, campaign_id, 'wells')
    ### FETCH_TAG add_wells

    ### FETCH_TAG add_well
    # @send_notification('wells') # update: dont use notification for each well - it makes things slow
    def add_well(self, well):

        collection = self.__get_collection('wells')
        collection_name = DbCollections().wells
        try:
            r = collection.insert_one(well)
        except pymongo.errors.WriteError as e:
            raise RuntimeError('FFCS_DB write error: Document failed validation for collection {}. '
                               'One of the required elements is missing or has wrong type. '
                               'For correct schema refer to: '
                               'https://git.psi.ch/mx/ffcs/tree/master/ffcs_db.\n'
                               'You tried to insert:\n {}'.format(collection_name, well))
        return r
    ### FETCH_TAG add_well

    ### FETCH_TAG add_campaign_library
    def add_campaign_library(self, campaign_library):
    
        collection = self.__get_collection('campaign_libraries')
        collection_name = DbCollections().campaign_libraries
    
        try:
            r = collection.insert_one(campaign_library)
        except pymongo.errors.WriteError as e:
            raise RuntimeError('FFCS_DB write error: Document failed validation for collection {}. '
                               'One of the required elements is missing or has wrong type. '
                               'For correct schema refer to: '
                               'https://git.psi.ch/mx/ffcs/tree/master/ffcs_db.\n'
                               'You tried to insert:\n {}'.format(collection_name, campaign_library))
        return r
    ### FETCH_TAG add_campaign_library

    ### FETCH_TAG get_all_wells
    def get_all_wells(self, user_account, campaign_id):
        query = {'userAccount': user_account, 'campaignId': campaign_id}
        collection = self.__get_collection('wells')
        r = collection.find(query)
        wells = list(r)
        # Convert the ObjectId to string
        for well in wells:
            well["_id"] = str(well["_id"])
            if well["libraryId"]:
                well["libraryId"] = str(well["libraryId"])
        return wells
    ### FETCH_TAG get_all_wells

    ### FETCH_TAG get_wells_from_plate
    def get_wells_from_plate(self, user_account, campaign_id, plate_id, **kwargs):
        query = {'userAccount': user_account, 'plateId': plate_id, 'campaignId': campaign_id}
        # query = {**query, **kwargs}
        query = self.__merge_two_dictionaries(query, kwargs)  # for python 2.* compatibility
        collection = self.__get_collection('wells')
        r = collection.find(query)
        listr = list(r)
        return listr
    ### FETCH_TAG get_wells_from_plate

    ### FETCH_TAG get_one_well
    def get_one_well(self, well_id):
        query = {'_id': well_id}
        collection = self.__get_collection('wells')
        r = collection.find_one(query)
        return r
    ### FETCH_TAG get_one_well
    
    ### FETCH_TAG get_one_campaign_library
    def get_one_campaign_library(self, library_id):
        query = {'_id': library_id}
        collection = self.__get_collection('campaign_libraries')  # note the collection name
        r = collection.find_one(query)
        return r
    ### FETCH_TAG get_one_campaign_library

    ### FETCH_TAG get_one_library
    def get_one_library(self, library_id):
        """
        Retrieves a single library record from the 'libraries' collection in the database using the given library ID.
    
        This function queries the database for the library record matching the provided library ID. If found, 
        it returns the library record as a dictionary. If no record is found, it returns None.
    
        Args:
            library_id: The identifier of the library to be retrieved. This should be an ObjectId.
    
        Returns:
            dict or None: The library record as a dictionary if found, otherwise None.
        """
        query = {'_id': library_id}
        collection = self.__get_collection('libraries')
        record = collection.find_one(query)
        return record
    ### FETCH_TAG get_one_library

    ### FETCH_TAG get_smiles
    def get_smiles(self, user_account, campaign_id, xtal_name):
        """
        Retrieves the SMILES string for a specific crystal from the 'wells' collection in the database.
    
        This function performs a query using the user's account, campaign ID, and crystal name. It fetches the
        corresponding record from the database and returns the 'smiles' string associated with that record. If the record
        does not exist, it returns None.
    
        Args:
            user_account (str): The identifier of the user account.
            campaign_id (str): The identifier of the campaign.
            xtal_name (str): The name of the crystal.
    
        Returns:
            str or None: The SMILES string if found, otherwise None.
    
        Raises:
            TypeError: If the record is not found and a NoneType access attempt occurs.
        """
        query = {'userAccount': user_account, 'campaignId': campaign_id, 'xtalName': xtal_name}
        collection = self.__get_collection('wells')
        record = collection.find_one(query)
        try:
            return record['smiles']
        except TypeError:  # Record is None because the document does not exist
            return None
    ### FETCH_TAG get_smiles

    ###
    ### Methods for Libraries
    ###
    
    ### FETCH_TAG import_library
    def import_library(self, library: dict) -> dict:
        """
        Imports a new library into the 'libraries' collection in the database.
    
        This function inserts a new library record into the database. The '_id' of the library is set to the library's barcode to ensure uniqueness.
        If a library with the same barcode already exists, a DuplicateKeyError is raised, indicating that the library is already imported.
    
        Args:
            library (dict): The library data to be imported. The library barcode is used as the '_id' in the database.
    
        Returns:
            dict: A dictionary with keys 'ok' and '_id', where 'ok' indicates the success of the operation and '_id' is the ObjectId of the inserted library.
    
        Raises:
            LibraryAlreadyImported: If a library with the same barcode already exists in the database.
        """
        collection = self.__get_collection('libraries')
        library['_id'] = library['libraryBarcode']
        try:
            result = collection.insert_one(library)
            # Mimic the old insert result structure
            old_result_format = {'ok': 1.0 if result.acknowledged else 0.0, '_id': result.inserted_id}
        except pymongo.errors.DuplicateKeyError:
            raise LibraryAlreadyImported(f'Library with name "{library["_id"]}" is already imported')
        return old_result_format
    ### FETCH_TAG import_library

    ### FETCH_TAG get_libraries
    def get_libraries(self, **kwargs):
        """
        Retrieves a list of all libraries from the 'libraries' collection in the database.
    
        This function queries the database for all entries in the 'libraries' collection and returns them.
        The current implementation does not utilize the 'kwargs' parameter, but it's included for future 
        extensibility where specific filtering based on user account or other criteria might be added.
    
        Args:
            **kwargs: A placeholder for future arguments for filtering the results.
    
        Returns:
            List[dict]: A list of dictionaries, each representing a library in the database.
    
        Notes:
            In the current implementation, no filtering is applied, and all libraries are returned.
            The '_id' field of each library is automatically generated by MongoDB.
        """
        collection = self.__get_collection('libraries')
        result = collection.find({})
        return list(result)
    ### FETCH_TAG get_libraries

    ### FETCH_TAG insert_campaign_library
    @send_notification('library')
    def insert_campaign_library(self, library: dict):
        """
        Inserts a new campaign library into the 'campaign_libraries' collection.
    
        This function adds a new library to the collection after removing any existing '_id' to avoid conflicts. 
        It ensures that the library data contains 'userAccount' and 'campaignId'. If these fields are missing, 
        it raises a RuntimeWarning. The function returns the raw result of the MongoDB insertion operation.
    
        Args:
            library (dict): The library data to be inserted.
    
        Returns:
            pymongo.results.InsertOneResult: The raw result of the database insertion operation.
    
        Raises:
            RuntimeWarning: If the library data does not contain 'userAccount' or 'campaignId'.
        """
        collection = self.__get_collection('campaign_libraries')
        library.pop('_id', None)
        if 'userAccount' not in library or 'campaignId' not in library:
            raise RuntimeWarning('ffcsdbclient: Library needs to have userAccount and CampaignId')
        return collection.insert_one(library)
    ### FETCH_TAG insert_campaign_library

    ### FETCH_TAG get_campaign_libraries
    def get_campaign_libraries(self, user: str, campaign_id: str) -> list:
        """
        Retrieves all libraries associated with a given user and campaign ID from the 'campaign_libraries' collection.
    
        This function queries the database for libraries matching the specified user and campaign ID,
        and returns a list of these libraries. 
    
        Args:
            user (str): The identifier of the user account.
            campaign_id (str): The identifier of the campaign.
    
        Returns:
            list: A list of dictionaries, each representing a library associated with the given user and campaign ID.
                  Each dictionary contains the details of a library.
    
        Raises:
            RuntimeError: If an error occurs during the database query.
        """
        try:
            collection = self.__get_collection('campaign_libraries')
            query = {'userAccount': user, 'campaignId': campaign_id}
            result = collection.find(query)
            return list(result)
        except Exception as e:
            raise RuntimeError(f"Error retrieving campaign libraries: {e}")
    ### FETCH_TAG get_campaign_libraries

    ### FETCH_TAG count_libraries_in_campaign
    def count_libraries_in_campaign(self, user, campaign_id, library_id):
        ### Alex: this apparently does exactly the same as get_library_usage_count
        ### This I just implemented one and mapped the other to it in ../ffcs_db_server/ffcs_db_server.py
        collection = self.__get_collection('wells')
        # match = {'userAccount': user, 'campaignId': campaign_id}
        # group = {'_id': '$libraryId', 'count': {'$sum': 1}}
        # query = [{'$match': match}, {'$group': group}]
        # r = collection.aggregate(query)
        # return list(r)
        query = {'userAccount': user, 'campaignId': campaign_id, 'libraryId': library_id }
        r = collection.find(query)
        return len(list(r))
    ### FETCH_TAG count_libraries_in_campaign

    ### FETCH_TAG get_library_usage_count
    def get_library_usage_count(self, user, campaign_id, library_id):
        """
        Retrieves the count of wells in the database associated with a specific library, user account, 
        and campaign. This method queries the 'wells' collection to count wells that match the given criteria.
    
        Args:
            user (str): The user account identifier.
            campaign_id (str): The campaign identifier.
            library_id (str): The library identifier.
    
        Returns:
            int: The count of wells matching the specified criteria.
    
        Raises:
            RuntimeError: If any error occurs during database access or query execution.
        """
        collection = self.__get_collection('wells')
        query = {'userAccount': user, 'campaignId': campaign_id, 'libraryId': library_id}
    
        try:
            result = collection.find(query)
            return len(list(result))
        except Exception as e:
            raise RuntimeError(f"Error retrieving library usage count: {e}")
    ### FETCH_TAG get_library_usage_count


    ###
    ### Group of methods for well - ligand interactions
    ###

    ### FETCH_TAG get_not_matched_wells
    def get_not_matched_wells(self, user, campaign_id):
        """
        Retrieves wells from the database that are not matched based on specific criteria. These wells are filtered
        by user account and campaign ID and further based on the 'compoundCode' and 'cryoProtection' status.
    
        This function queries the 'wells' collection and finds wells where 'compoundCode' is None and either 
        'cryoProtection' is False, or 'cryoProtection' is True with a 'cryoStatus' of 'exported'.
    
        Args:
            user (str): The user account identifier.
            campaign_id (str): The campaign identifier.
    
        Returns:
            List[dict]: A list of dictionaries, each representing a well that matches the query criteria.
    
        Raises:
            RuntimeError: If there is an issue with database access or query execution.
        """
        collection = self.__get_collection('wells')
    
        query = {
            'userAccount': user, 
            'campaignId': campaign_id, 
            'compoundCode': None,
            '$or': [{'cryoProtection': False}, {'cryoProtection': True, 'cryoStatus': 'exported'}]
        }
    
        try:
            result = collection.find(query)
            return list(result)
        except Exception as e:
            raise RuntimeError(f"Error retrieving not matched wells: {e}")
    ### FETCH_TAG get_not_matched_wells

    ### FETCH_TAG add_fragment_to_well
    def add_fragment_to_well(self, library, well_id, fragment, solvent_volume, ligand_transfer_volume,
                             ligand_concentration, is_solvent_test=False):
        """
        Adds a fragment to a specified well in the database. It updates various fields in the well document
        and, if applicable, marks the fragment as used in the corresponding library document.
    
        Args:
            library (dict): A dictionary containing information about the library.
            well_id (ObjectId): The MongoDB ObjectId of the well to which the fragment is to be added.
            fragment (dict): A dictionary containing information about the fragment.
            solvent_volume (float): The volume of the solvent.
            ligand_transfer_volume (float): The volume of the ligand transfer.
            ligand_concentration (float): The concentration of the ligand.
            is_solvent_test (bool, optional): A flag indicating if the well is used for solvent testing. Defaults to False.
    
        Returns:
            dict: A dictionary indicating the result of the database update operation. It contains 'nModified' for the count
                  of modified documents, 'ok' to indicate success, and 'n' for the count of matched documents.
    
        Raises:
            RuntimeError: If any exception occurs during the database operations.
        """
        collection = self.__get_collection('wells')
        library_collection = self.__get_collection('campaign_libraries')
    
        # Prepare library and fragment information for update
        library_name = library['libraryName']
        library_barcode = library['libraryBarcode']
        library_id = library['_id']
        library_concentration = fragment.get('libraryConcentration', 'n/a')
        source_well = fragment['well']
        smiles = fragment['smiles']
        compound_code = fragment['compoundCode']
    
        # Update query for the well
        query = {'_id': well_id}
        update = {'$set': {'libraryName': library_name,
                           'libraryBarcode': library_barcode,
                           'libraryId': library_id,
                           'solventTest': is_solvent_test,
                           'sourceWell': source_well,
                           'libraryAssigned': True,
                           'compoundCode': compound_code,
                           'smiles': smiles,
                           'libraryConcentration': library_concentration,
                           'solventVolume': solvent_volume,
                           'ligandTransferVolume': ligand_transfer_volume,
                           'ligandConcentration': ligand_concentration,
                           'soakStatus': 'pending'}}
    
        try:
            # Perform the update operation on the well
            well_update_result = collection.update_one(query, update)
    
            # Update library if not a solvent test
            if not is_solvent_test:
                query_library = {'_id': library_id, 'fragments.compoundCode': compound_code}
                update_library = {'$set': {'fragments.$.used': True}}
                library_update_result = library_collection.update_one(query_library, update_library)
    
            # Prepare the result in the old format for compatibility
            old_result_format = {'nModified': well_update_result.modified_count,
                                 'ok': 1.0 if well_update_result.acknowledged else 0.0,
                                 'n': well_update_result.matched_count}
            return old_result_format
        except Exception as e:
            raise RuntimeError(f"Error adding fragment to well: {e}")
    ### FETCH_TAG add_fragment_to_well

    ### FETCH_TAG remove_fragment_from_well
    def remove_fragment_from_well(self, well_id):
        """
        Removes the fragment from a specified well in the database. This involves setting various
        library-related fields to None or False, indicating the removal of the fragment.
    
        Args:
            well_id (ObjectId): The MongoDB ObjectId of the well from which the fragment is to be removed.
    
        Returns:
            dict: A dictionary indicating the result of the database update operation. It contains 
                  'nModified' for the count of modified documents, 'ok' to indicate success, and 
                  'n' for the count of matched documents.
    
        Raises:
            RuntimeError: If any exception occurs during the database operations.
        """
        collection = self.__get_collection('wells')
        library_collection = self.__get_collection('campaign_libraries')
    
        query = {'_id': well_id}
        update = {
            '$set': {
                'libraryName': None,
                'libraryBarcode': None,
                'libraryId': None,
                'sourceWell': None,
                'libraryAssigned': False,
                'solventTest': False,
                'compoundCode': None,
                'smiles': None,
                'libraryConcentration': None,
                'solventVolume': None,
                'ligandTransferVolume': None,
                'ligandConcentration': None,
                'soakStatus': None
            }
        }
    
        try:
            # Get well status before removal and find library info associated with it
            well = self.get_one_well(well_id)
            library_id = well.get('libraryId')
            compound_code = well.get('compoundCode')
    
            query_library = {'_id': library_id, 'fragments.compoundCode': compound_code}
            update_library = {'$set': {'fragments.$.used': False}}
    
            # Perform update operations
            r = collection.update_one(query, update)
            library_collection.update_one(query_library, update_library)
    
            # Mimic the old update result structure
            old_result_format = {
                'nModified': r.modified_count, 
                'ok': 1.0 if r.acknowledged else 0.0, 
                'n': r.matched_count
            }
            return old_result_format
        except Exception as e:
            raise RuntimeError(f"Error removing fragment from well: {e}")
    ### FETCH_TAG remove_fragment_from_well

    ### FETCH_TAG get_id_of_plates_to_soak
    def get_id_of_plates_to_soak(self, user, campaign_id):
        """
        Retrieves a list of plate IDs along with the count of wells with and without an assigned 
        library for each plate, filtered by user account and campaign ID.
    
        This function performs a MongoDB aggregation to find plates matching the specified user 
        account and campaign ID. It then aggregates data to count the total wells, wells with 
        a library assigned, and wells without a library assigned for each plate.
    
        Args:
            user (str): The user account identifier.
            campaign_id (str): The campaign identifier.
    
        Returns:
            List[Dict[str, Union[str, int]]]: A list of dictionaries, each containing the plate ID,
            total well count, count of wells with a library, and count of wells without a library.
    
        Raises:
            RuntimeError: If there is an issue with database access or query execution.
        """
        collection = self.__get_collection('wells')
    
        match = {
            'userAccount': user, 
            'campaignId': campaign_id,
            '$or': [{'soakStatus': 'pending'}, {'soakStatus': None}]
        }
    
        group = {
            '_id': '$plateId',
            'totalWells': {'$sum': 1},
            'wellsWithLibrary': {'$sum': {'$cond': ['$libraryAssigned', 1, 0]}},
            'wellsWithoutLibrary': {'$sum': {'$cond': ['$libraryAssigned', 0, 1]}}
        }
    
        try:
            result = collection.aggregate([
                {'$match': match},
                {'$sort': {'_id': -1}},
                {'$group': group}
            ])
            return list(result)
        except Exception as e:
            raise RuntimeError(f"Error retrieving plate IDs for soak: {e}")
    ### FETCH_TAG get_id_of_plates_to_soak

    ### FETCH_TAG get_id_of_plates_to_cryo_soak
    def get_id_of_plates_to_cryo_soak(self, user, campaign_id):
        """
        Retrieves a list of plate IDs along with the count of wells with and without cryo protection 
        for each plate, filtered by user account and campaign ID.
    
        This function performs a MongoDB aggregation to find plates matching the specified user 
        account and campaign ID, and then counts the number of wells with and without cryo 
        protection for each plate.
    
        Args:
            user (str): The user account identifier.
            campaign_id (str): The campaign identifier.
    
        Returns:
            List[Dict[str, Union[str, int]]]: A list of dictionaries, each containing the plate ID,
            total well count, count of wells with cryo protection, and count of wells without cryo protection.
    
        Raises:
            RuntimeError: If there is an issue with database access or query execution.
        """
        collection = self.__get_collection('wells')
    
        match = {
            'userAccount': user, 
            'campaignId': campaign_id,
            '$or': [{'cryoStatus': 'pending'}, {'cryoStatus': None}]
        }
    
        group = {
            '_id': '$plateId',
            'totalWells': {'$sum': 1},
            'wellsWithCryoProtection': {'$sum': {'$cond': ['$cryoProtection', 1, 0]}},
            'wellsWithoutCryoProtection': {'$sum': {'$cond': ['$cryoProtection', 0, 1]}}
        }
    
        try:
            result = collection.aggregate([
                {'$match': match},
                {'$sort': {'_id': -1}},
                {'$group': group}
            ])
            return list(result)
        except Exception as e:
            raise RuntimeError(f"Error retrieving plate IDs for cryo soak: {e}")
    ### FETCH_TAG get_id_of_plates_to_cryo_soak

    ### FETCH_TAG get_id_of_plates_for_redesolve
    def get_id_of_plates_for_redesolve(self, user, campaign_id):
        """
        Retrieves the IDs of plates for redesolve operation, along with the count of wells 
        with and without new solvent for each plate, filtered by user account and campaign ID.
    
        This function queries a MongoDB collection to find plates that match the given user 
        account and campaign ID. It aggregates data to count total wells, wells with new 
        solvent, and wells without new solvent for each plate.
    
        Args:
            user (str): The user account identifier.
            campaign_id (str): The campaign identifier.
    
        Returns:
            List[Dict[str, Union[str, int]]]: A list of dictionaries, each containing the plate ID, 
            total well count, count of wells with new solvent, and count of wells without new solvent.
    
        Raises:
            RuntimeError: If there is an issue with database access or query execution.
        """
        collection = self.__get_collection('wells')
        
        # Define the match criteria for MongoDB aggregation
        match = {
            'userAccount': user, 
            'campaignId': campaign_id,
            '$or': [{'redesolveStatus': 'pending'}, {'redesolveStatus': None}]
        }
    
        sort = {'_id': -1}
    
        # Define the group criteria for MongoDB aggregation
        group = {
            '_id': '$plateId',
            'totalWells': {'$sum': 1},
            'wellsWithNewSolvent': {'$sum': {'$cond':['$redesolveApplied', 1, 0]}},
            'wellsWithoutNewSolvent': {'$sum': {'$cond': ['$redesolveApplied', 0, 1]}}
        }
    
        try:
            result = collection.aggregate([{'$match': match}, {'$sort': sort}, {'$group': group}])
            return list(result)
        except Exception as e:
            raise RuntimeError(f"Error retrieving plate IDs for redesolve: {e}")
    ### FETCH_TAG get_id_of_plates_for_redesolve

    ### FETCH_TAG export_to_soak_selected_wells
    def export_to_soak_selected_wells(self, user, campaign_id, data):
        """
        Marks selected wells as exported by updating their 'soakExportTime' and 'soakStatus'.

        This function identifies wells based on the provided user and campaign ID that have not yet been exported and
        have 'libraryAssigned' set to True. It updates these wells, setting 'soakExportTime' to the current time and
        'soakStatus' to 'exported'.

        Args:
            user (str): The user account associated with the wells.
            campaign_id (str): The campaign ID associated with the wells.
            data (list of dict): Each dict contains the 'plateId' of a well to update.

        Returns:
            None: The function performs database updates but does not return a value.

        Raises:
            Exception: If any database operation fails.
        """
        wells_collection = self.__get_collection('wells')
        now = datetime.datetime.now()

        for well in data:
            if 'plateId' not in well:
                raise ValueError("Each item in data must include a 'plateId'.")
            
            query = {
                'userAccount': user,
                'campaignId': campaign_id,
                'plateId': well['plateId'],
                'soakExportTime': None,
                'libraryAssigned': True,
                'soakStatus': 'pending'
            }
            update = {
                '$set': {
                    'soakExportTime': now,
                    'soakStatus': 'exported'
                }
            }

            try:
                wells_collection.update_many(query, update)
            except Exception as e:
                raise Exception(f"Database update operation failed: {e}")

        # Send a notification to the user about the update.
        self.send_notification(user, campaign_id, 'wells')
    ### FETCH_TAG export_to_soak_selected_wells

    ### FETCH_TAG export_cryo_to_soak_selected_wells
    def export_cryo_to_soak_selected_wells(self, user, campaign_id, data):
        """
        Updates the cryoExportTime to the current time and cryoStatus to 'exported' for the selected wells.
    
        This function is invoked to mark selected wells that are ready to have their 'cryo' data exported.
        It updates the 'cryoExportTime' with the current timestamp and sets the 'cryoStatus' to 'exported'.
        It filters the wells based on the provided user and campaign ID and updates only those wells that
        have 'cryoProtection' set to True and 'cryoStatus' set to 'pending'.
    
        Args:
            user: The username associated with the wells.
            campaign_id: The identifier of the campaign associated with the wells.
            data: A list of dictionaries where each dictionary represents a selected well with its plate ID.
    
        Returns:
            None: The function returns nothing but performs updates on the database and may send a notification.
    
        Raises:
            Exception: If there is an issue performing the update operation on the database.
        """
        wells_collection = self.__get_collection('wells')
        now = datetime.datetime.now()
    
        for well in data:
            # Construct query to find non-exported plates only
            query = {
                'userAccount': user,
                'campaignId': campaign_id,
                'plateId': well['plateId'],
                'cryoExportTime': None,
                'cryoProtection': True,
                'cryoStatus': 'pending'
            }
            update = {
                '$set': {
                    'cryoExportTime': now,
                    'cryoStatus': 'exported'
                }
            }
    
            try:
                # Perform the update operation on the wells collection
                wells_collection.update_many(query, update, False)
            except Exception as e:
                raise Exception(f"Database update operation failed: {e}")
    
        # Optionally send a notification after the update operation
        self.send_notification(user, campaign_id, 'wells')
    ### FETCH_TAG export_cryo_to_soak_selected_wells

    ### FETCH_TAG export_redesolve_to_soak_selected_wells
    def export_redesolve_to_soak_selected_wells(self, user, campaign_id, data):
        """
        Updates 'redesolveExportTime' and 'redesolveStatus' for selected wells based on 'redesolve' data.
    
        Iterates through a list of wells and updates the database to mark 'redesolveExportTime' with the
        current timestamp and 'redesolveStatus' as 'exported'. This update is only applied to wells that are
        pending export and have not been exported yet.
    
        Args:
            user: The username associated with the wells.
            campaign_id: The identifier of the campaign to which the wells belong.
            data: A list of dictionaries, each representing a well with at least a 'plateId' key.
    
        Returns:
            None: This function does not return any value.
    
        Raises:
            ValueError: If an item in 'data' does not contain the required 'plateId' key.
            Exception: If the database operation fails for any reason.
        """
        wells_collection = self.__get_collection('wells')
        now = datetime.datetime.now()
    
        for well in data:
            if not isinstance(well, dict) or 'plateId' not in well:
                raise ValueError('Each item in data should be a dict with a "plateId" key.')
            
            # this will find only non exported plates
            query = {
                'userAccount': user,
                'campaignId': campaign_id,
                'plateId': well['plateId'],
                'redesolveExportTime': None,
                'redesolveApplied': True,
                'redesolveStatus': 'pending'
            }
            update = {
                '$set': {
                    'redesolveExportTime': now,
                    'redesolveStatus': 'exported'
                }
            }
    
            # Attempt to update the database and handle potential exceptions.
            try:
                r = wells_collection.update_many(query, update)
            except Exception as e:
                raise Exception(f"Database update operation failed: {e}")
    
        # Send a notification regarding the update
        self.send_notification(user, campaign_id, 'wells')
    ### FETCH_TAG export_redesolve_to_soak_selected_wells

    ### FETCH_TAG export_to_soak
    def export_to_soak(self, data):
        """
        Marks soakExportTime in each Well in Wells collection and each plate in Plates collection.
        
        Args:
            data (List[Dict[str, Union[str, datetime]]]): A list of dictionaries, each containing
                the plate ID and the soak export time. Expected format:
                [{'_id': str, 'soak_time': datetime_object}, ...].
        
        Returns:
            pymongo.results.UpdateResult: The result of the update operation on the plates collection.
        
        Raises:
            ValueError: If 'data' is empty or if any entry lacks the '_id' or 'soak_time' keys.
            Exception: If database update operations fail.
        """
        if not data:
            raise ValueError("Input data is empty.")
        
        for item in data:
            if not isinstance(item, dict) or '_id' not in item or 'soak_time' not in item:
                raise ValueError("Each item in data must be a dict with '_id' and 'soak_time' keys.")
        
        wells_collection = self.__get_collection('wells')
        plates_collection = self.__get_collection('plates')
        
        for plate in data:
            query = {'plateId': plate['_id'], 'soakExportTime': None, 'libraryAssigned': True}
            query_plates = {'plateId': plate['_id']}
            update = {'$set': {'soakExportTime': plate['soak_time'], 'soakStatus': 'exported'}}
        
            try:
                r = wells_collection.update_many(query, update)
                r_p = plates_collection.update_one(query_plates, update)
            except Exception as e:
                raise Exception(f"Database operation failed: {e}")
        
        return r_p
    ### FETCH_TAG export_to_soak

    ### FETCH_TAG export_cryo_to_soak
    def export_cryo_to_soak(self, data):
        """
        Marks cryoExportTime in each Well in Wells collection and each plate in Plates collection.
    
        Args:
            data (list): A list of dictionaries, each containing the plate ID and the soak export time.
                         Expected format: [{'_id': str, 'soak_time': datetime_object}, ...]
    
        Returns:
            pymongo.results.UpdateResult: The result of the update operation on the wells collection.
    
        Raises:
            ValueError: If 'data' is empty or if any entry lacks the '_id' or 'soak_time' keys.
            Exception: If database update operations fail.
        """
        if not data:
            raise ValueError("Input data is empty.")
    
        for item in data:
            if not isinstance(item, dict) or '_id' not in item or 'soak_time' not in item:
                raise ValueError("Each item in data must be a dict with '_id' and 'soak_time'.")
    
        wells_collection = self.__get_collection('wells')
        plates_collection = self.__get_collection('plates')
    
        for plate in data:
            query = {'plateId': plate['_id'], 'cryoExportTime': None, 'cryoProtection': True}
            update = {
                '$set': {
                    'cryoExportTime': plate['soak_time'], 
                    'cryoStatus': 'exported'
                }
            }
            query_plates = {'plateId': plate['_id']}
            update_plates = {'$set': {'cryoProtection': True}}
    
            try:
                wells_result = wells_collection.update_many(query, update)
                plates_result = plates_collection.update_one(query_plates, update_plates)
            except Exception as e:
                raise Exception(f"Database operation failed: {e}")
    
        return wells_result
    ### FETCH_TAG export_cryo_to_soak

    ### FETCH_TAG export_redesolve_to_soak
    def export_redesolve_to_soak(self, data):
        """
        Marks the `redesolveExportTime` for wells and plates as exported based on the given data.
    
        This method updates the `redesolveExportTime` and `redesolveStatus` for wells, and
        `redesolveApplied` for plates in their respective collections to reflect the exportation
        of 'redesolve' data based on the provided plate information.
    
        Args:
            data (List[Dict[str, Union[str, datetime]]]): A list of dictionaries containing
                the plate ID and the timestamp when the data was soaked.
    
        Returns:
            UpdateResult: The result of the update operation from the wells collection.
    
        Raises:
            ValueError: If `data` is empty or incorrectly formatted.
            Exception: If there are issues updating the database collections.
        """
        # Validate input data
        if not data or not all('_id' in item and 'soak_time' in item for item in data):
            raise ValueError("Each dictionary in `data` must contain '_id' and 'soak_time' keys")
    
        wells_collection = self.__get_collection('wells')
        plates_collection = self.__get_collection('plates')
    
        for plate in data:
            # Construct the query and update dictionaries for wells and plates
            well_query = {'plateId': plate['_id'], 'redesolveExportTime': None, 'redesolveApplied': True}
            well_update = {'$set': {'redesolveExportTime': plate['soak_time'], 'redesolveStatus': 'exported'}}
            plate_query = {'plateId': plate['_id']}
            plate_update = {'$set': {'redesolveApplied': True}}
    
            # Execute the update operations
            try:
                well_result = wells_collection.update_many(well_query, well_update)
                plate_result = plates_collection.update_one(plate_query, plate_update)
            except Exception as e:
                raise Exception(f"Database update operation failed: {e}")
    
        # Return the result of the update operation for wells
        return well_result
    ### FETCH_TAG export_redesolve_to_soak

    ### FETCH_TAG import_soaking_results
    def import_soaking_results(self, wells_data):
        """
        Imports soaking results by updating the SoakStatus of wells to 'Done'.
    
        Processes a list of well data, each containing a 'plateId', 'wellEcho', and 'transferStatus'.
        It updates the SoakStatus of each well, finds the corresponding user and campaign,
        marks the soak for the well in echo as done, and sends a notification.
    
        Args:
            wells_data (List[Dict[str, Any]]): A list of dictionaries, each representing well data
                                                with keys 'plateId', 'wellEcho', and 'transferStatus'.
    
        Raises:
            ValueError: If wells_data is empty or None.
            RuntimeError: If the user data is not found.
        """
        if not wells_data:
            raise ValueError("wells_data list is empty or None.")
    
        # Find user and campaign for given well based on plate_id from 1st element
        # Assumes that all the plates in wells_data belong to the same user and same campaign
        plate_id = wells_data[0]['plateId']
        user_data = self.find_user_from_plate_id(plate_id)
        if user_data is None:
            raise RuntimeError(f"No user data found for plate_id {plate_id}")
    
        user = user_data['user']
        campaign_id = user_data['campaign_id']
    
        # Update soak status for each well in the data
        for well in wells_data:
            plate_id = well['plateId']
            well_echo = well['wellEcho']
            transfer_status = well['transferStatus']
            self.mark_soak_for_well_in_echo_done(user, campaign_id, plate_id, well_echo, transfer_status)
                    
        # When all updates are done, send notification
        self.send_notification(user, campaign_id, 'wells')
    ### FETCH_TAG import_soaking_results

    ### FETCH_TAG mark_soak_for_well_in_echo_done
    def mark_soak_for_well_in_echo_done(self, user, campaign_id, plate_id, well_echo, transfer_status):
        """
        Marks a well's soak status as 'done' after a transfer in Echo is completed.
    
        Updates the 'soakStatus' to 'done', sets the 'soakTransferTime' to the current time,
        and updates the 'soakTransferStatus' with the provided status.
    
        Args:
            user (str): The user account performing the update.
            campaign_id (str): The campaign identifier associated with the well.
            plate_id (str): The plate identifier where the well is located.
            well_echo (str): The specific well identifier in Echo.
            transfer_status (str): The status of the transfer operation.
    
        Returns:
            pymongo.results.UpdateResult: The result of the update operation containing the number
            of documents matched and modified.
    
        Raises:
            ValueError: If any of the parameters are missing or None.
        """
        # Validation of input parameters
        if not all([user, campaign_id, plate_id, well_echo, transfer_status]):
            raise ValueError("All parameters are required and cannot be None.")
    
        wells_collection = self.__get_collection('wells')
        now = datetime.datetime.now()
        query = {
            'userAccount': user,
            'campaignId': campaign_id,
            'plateId': plate_id,
            'wellEcho': well_echo,
            'soakStatus': 'exported'
        }
        update = {
            '$set': {
                'soakStatus': 'done',
                'soakTransferTime': now,
                'soakTransferStatus': transfer_status
            }
        }
        try:
            # Perform the update operation on the wells collection
            update_result = wells_collection.update_many(query, update)
            return update_result
        except Exception as e:
            ### You may log the error here if a logging system is in place.
            # For example: logger.error(f"Failed to mark soak for well in Echo done: {e}")
            raise  # Re-raise the exception for the caller to handle
    ### FETCH_TAG mark_soak_for_well_in_echo_done

    ###
    ### Group of methods for Cryo Protection
    ###

    ### FETCH_TAG add_cryo
    def add_cryo(self, user_account, campaign_id, target_plate, target_well,
                 cryo_desired_concentration, cryo_transfer_volume,
                 cryo_source_well, cryo_name, cryo_barcode):
        """
        Adds cryoprotection details to the well record in the database.
    
        This function updates the database to indicate that cryoprotection has been
        added to a well. It sets various cryoprotection-related fields and changes
        the status to 'pending'.
    
        Args:
            user_account (str): The user account identifier.
            campaign_id (str): The campaign identifier.
            target_plate (str): The plate identifier where the well is located.
            target_well (str): The well identifier to be updated.
            cryo_desired_concentration (float): The desired concentration of cryoprotectant.
            cryo_transfer_volume (float): The volume of cryoprotectant to be transferred.
            cryo_source_well (str): The source well identifier for the cryoprotectant.
            cryo_name (str): The name of the cryoprotectant.
            cryo_barcode (str): The barcode associated with the cryoprotectant.
    
        Returns:
            pymongo.results.UpdateResult: The result of the update operation.
    
        Raises:
            ValueError: If the update operation does not affect any documents.
        """
        query = {
            'userAccount': user_account,
            'campaignId': campaign_id,
            'plateId': target_plate,
            'well': target_well
        }
        update = {
            '$set': {
                'cryoProtection': True,
                'cryoDesiredConcentration': cryo_desired_concentration,
                'cryoTransferVolume': cryo_transfer_volume,
                'cryoSourceWell': cryo_source_well,
                'cryoName': cryo_name,
                'cryoBarcode': cryo_barcode,
                'cryoStatus': 'pending'
            }
        }
        wells_collection = self.__get_collection('wells')
    
        result = wells_collection.update_many(query, update)
        if result.matched_count == 0:
            raise ValueError("No documents matched the query. Cryoprotection details not added.")
        return result
    ### FETCH_TAG add_cryo

    ### FETCH_TAG remove_cryo_from_well
    def remove_cryo_from_well(self, well_id):
        """
        Removes cryoprotectant data from the specified well in the database.
    
        This function updates the well document with the given `well_id` by setting 
        various cryo-related fields to `None` and `cryoProtection` to `False`.
    
        Args:
            well_id (ObjectId): The unique identifier of the well to update.
    
        Returns:
            UpdateResult: The result of the update operation from the MongoDB collection.
    
        Note:
            The `well_id` parameter is expected to be of type `ObjectId`.
            If the update operation fails, a `WriteError` may be raised, which is not
            explicitly handled in this function.
        """
        collection = self.__get_collection('wells')
        query = {'_id': well_id}
        update = {
            '$set': {
                'cryoName': None,
                'cryoBarcode': None,
                'cryoSourceWell': None,
                'cryoProtection': False,
                'cryoStockConcentration': None,
                'cryoDesiredConcentration': None,
                'cryoTransferVolume': None,
                'cryoTransferConcentration': None,
                'cryoStatus': None
            }
        }
    
        # The `upsert` parameter is set to False; this does not change the original functionality.
        update_result = collection.update_one(query, update, upsert=False)
        return update_result
    ### FETCH_TAG remove_cryo_from_well

    ### FETCH_TAG remove_new_solvent_from_well
    def remove_new_solvent_from_well(self, well_id):
        """
        Removes the New Solvent (redissolve option) from the specified well.
    
        This method updates the well document by setting the fields associated with 
        the redissolve option to their default 'empty' states, which effectively 
        removes the redissolve information from the well.
    
        Args:
            well_id (ObjectId): The unique identifier for the well from which the 
                                New Solvent should be removed. Expected to be an instance 
                                of ObjectId, which represents the well's ID in the database.
    
        Returns:
            UpdateResult: An instance of UpdateResult indicating the outcome of the 
                          update operation.
    
        Raises:
            TypeError: If the 'well_id' is not an instance of ObjectId.
            PyMongoError: If the update operation fails.
    
        Note:
            The `well_id` parameter is expected to be of type ObjectId, which is not 
            a native Python type but is commonly used with MongoDB to represent unique 
            identifiers. The UpdateResult is a PyMongo class representing the result 
            of an update operation.
        """
        collection = self.__get_collection('wells')
    
        query = {'_id': well_id}
        update = {'$set': {'redesolveName': None,
                           'redesolveBarcode': None,
                           'redesolveSourceWell': None,
                           'redesolveApplied': False,
                           'redesolveTransferVolume': None,
                           'redesolveStatus': None}}
    
        result = collection.update_one(query, update, False)
        return result
    ### FETCH_TAG remove_new_solvent_from_well

    ### FETCH_TAG get_cryo_usage
    def get_cryo_usage(self, user, campaign_id):
        """
        Calculates the total cryo protection volume for each cryo source well in the 'wells' collection
        based on specific conditions.
    
        Parameters:
            user (str): The user account associated with the wells.
            campaign_id (str): The campaign ID associated with the wells.
    
        Returns:
            list: A list of dictionaries containing cryo source well IDs and their respective total cryo transfer volumes.
                  Example: [{ "_id" : {"sourceWell": "A2", "libraryName": "Lib1"}, "total" : 150 }]
    
        Raises:
            Exception: If unable to access MongoDB collection.
        """
        try:
            # MongoDB query to match wells with specific conditions
            match_query = {
                '$match': {
                    'userAccount': user,
                    'campaignId': campaign_id,
                    'cryoProtection': True,
                    'cryoStatus': {'$ne': 'exported'}
                }
            }
    
            # MongoDB query to group by cryo source well and library name, and sum cryo transfer volume
            group_query = {
                '$group': {
                    '_id': {'sourceWell': '$cryoSourceWell', 'libraryName': '$cryoName'},
                    'total': {'$sum': '$cryoTransferVolume'}
                }
            }
    
            # Get MongoDB 'wells' collection
            collection = self.__get_collection('wells')
    
            # Aggregate using the match and group queries
            aggregation_result = collection.aggregate([match_query, group_query])
            
            # Return aggregation result as list
            return list(aggregation_result)
    
        except Exception as e:
            ### If unable to access MongoDB collection, raise an exception
            raise Exception(f"An error occurred while accessing the MongoDB collection: {e}")
    ### FETCH_TAG get_cryo_usage

    ### FETCH_TAG get_solvent_usage
    def get_solvent_usage(self, user, campaing_id):
        """
        For wells that have the cryoProtection set to True and cryoStatus set to "pending",
        this function calculates the total ligandTransferVolume for each unique combination of
        sourceWell and libraryName.
        
        Parameters:
            user (str): The user account identifier.
            campaing_id (str): The campaign identifier.
            
        Returns:
            list: A list of dictionaries containing the unique combination of sourceWell and
                  libraryName and the total ligandTransferVolume. E.g.,
                  [{ "_id" : { "sourceWell": "A2", "libraryName": "Library Test" }, "total" : 150 }]
        
        Note:
            This function makes use of MongoDB's aggregation framework.
        """
        
        ### Construct the match stage of the aggregation pipeline
        match_stage = {
            '$match': {
                'userAccount': user,
                'campaignId': campaing_id,
                'solventTest': True,
                '$and': [
                    {'soakStatus': {'$ne': 'exported'}},
                    {'soakStatus': {'$ne': 'done'}}
                ]
            }
        }
        
        ### Construct the group stage of the aggregation pipeline
        group_stage = {
            '$group': {
                '_id': {
                    'sourceWell': '$sourceWell',
                    'libraryName': '$libraryName'
                },
                'total': {
                    '$sum': '$ligandTransferVolume'
                }
            }
        }
        
        ### Get the MongoDB collection
        collection = self.__get_collection('wells')
        
        ### Perform aggregation
        aggregation_pipeline = [match_stage, group_stage]
        aggregation_result = collection.aggregate(aggregation_pipeline)
        
        return list(aggregation_result)
    ### FETCH_TAG get_solvent_usage

    ###
    ### redesolve commands
    ###

    ### FETCH_TAG redesolve_in_new_solvent
    def redesolve_in_new_solvent(self, user_account, campaign_id, target_plate, target_well, redesolve_transfer_volume,
                                 redesolve_source_well, redesolve_name, redesolve_barcode):
        """
        Update well information for a given user, campaign, and plate with redesolve parameters.
    
        Parameters:
            self (object): Reference to the current instance.
            user_account (str): The account identifier for the user.
            campaign_id (str): The identifier for the campaign.
            target_plate (str): The identifier for the target plate.
            target_well (str): The identifier for the target well.
            redesolve_transfer_volume (int): The volume to be transferred during redesolve.
            redesolve_source_well (str): The identifier for the source well.
            redesolve_name (str): The name of the redesolve action.
            redesolve_barcode (str): The barcode associated with redesolve action.
    
        Returns:
            r (UpdateResult): The result of the database update operation.
        """
        ### Prepare the query and update dictionary
        query = {
            'userAccount': user_account,
            'campaignId': campaign_id,
            'plateId': target_plate,
            'well': target_well
        }
    
        update = {
            '$set': {
                'redesolveApplied': True,
                'redesolveTransferVolume': redesolve_transfer_volume,
                'redesolveSourceWell': redesolve_source_well,
                'redesolveName': redesolve_name,
                'redesolveBarcode': redesolve_barcode,
                'redesolveStatus': 'pending'
            }
        }
    
        ### Get the wells collection and perform the update
        wells_collection = self.__get_collection('wells')
        r = wells_collection.update_many(query, update, False)
    
        return r
    ### FETCH_TAG redesolve_in_new_solvent

    ### FETCH_TAG update_notes
    def update_notes(self, user, campaign_id, doc_id, note):
        """
        Update the 'notes' field of a well document in the 'Wells' collection.
    
        This function converts the doc_id to a BSON ObjectId and then delegates the actual update
        operation to the update_by_object_id method. This method is assumed to be defined elsewhere
        in this module and is used for updating a document based on its Object ID.
    
        Parameters:
        - user (str): The username of the person performing the update.
        - campaign_id (str): The ID of the campaign to which the well belongs.
        - doc_id (str): The document ID of the well to be updated, in string format.
        - note (str): The note text to be updated in the well document.
    
        Returns:
        - dict: A dictionary containing the result of the update operation, typically including keys like 'ok', 'nModified', etc.
    
        Raises:
        - Any exceptions raised by the update_by_object_id method will be propagated up to the caller.
    
        Example:
        >>> update_notes("e14965", "EP_SmarGon", "60c72b2f4b3d68d3ed170345", "This is a test note.")
        {'ok': 1.0, 'nModified': 1, 'n': 1}
    
        Note: The function assumes that update_by_object_id is a reliable method for document update, and that it properly
        handles all edge cases like non-existing IDs, unauthorized access, etc.
        """
    
        ### Convert the document ID to a BSON ObjectId
        try:
            doc_id = bson.ObjectId(doc_id)
        except bson.errors.InvalidId:
            return {"ok": 0.0, "error": "Invalid BSON ObjectId format"}
    
        ### Delegate the update operation to update_by_object_id
        return self.update_by_object_id(user, campaign_id, 'Wells', doc_id, notes=note)
    ### FETCH_TAG update_notes

    ###
    ### Shifter information
    ###

    ### FETCH_TAG import_fishing_results
    def import_fishing_results(self, fishing_results):
        """
        Import fishing results into the ffcs_db.
        
        This function iterates through an array of dictionaries containing fishing results, 
        extracts relevant information, and performs several database operations.
        
        After completing all operations, it sends a notification for the userAccount: 'shifter', 
        and campaignId: 'shifter'.
        
        :param fishing_results: An array of dictionaries containing processed CSV file data from 'shifter'.
        :return: The result of the last `update_shifter_fishing_result` operation.
        
        Raises:
            - RuntimeError: If the plateId is missing in the shifter result file.
        """
        
        ### Iterate through each well_data dictionary in the fishing_results array
        for well_data in fishing_results:
            ### Extract plateId from well_data, if it exists
            try:
                plate_id = well_data['plateId']
            except KeyError:
                raise RuntimeError(
                    'ffcsdbclient:import_fishing_results -> plateId is missing in the shifter result file.'
                )
    
            ### Generate the next xtal name index based on the plateId
            xtal_name_index = self.get_next_xtal_number(plate_id)
            
            ### Find user and campaign information based on the plateId
            user_info = self.find_user_from_plate_id(plate_id)
            campaign_id = user_info['campaign_id']
            
            ### Update the shifter fishing result
            r = self.update_shifter_fishing_result(
                well_data, xtal_name_index, xtal_name_prefix=campaign_id
            )
            
            ### Commented out: Optional sleep to ensure database write completion
            ### time.sleep(0.1) 
    
        ### Send a notification once all well data has been processed
        self.send_notification('shifter', 'shifter', 'wells')
        
        return r
    ### FETCH_TAG import_fishing_results

    ### FETCH_TAG find_user_from_plate_id
    def find_user_from_plate_id(self, plate_id):
        """
        Finds a user and campaign ID associated with a given plate ID.
        
        Args:
            plate_id (str): The ID of the plate to look for.
            
        Returns:
            dict: A dictionary containing the 'user' and 'campaign_id' if the plate is found.
            None: If the plate is not found.
            
        Raises:
            RuntimeError: If an unexpected issue occurs while fetching data.
        """
        try:
            collection = self.__get_collection('plates')
            
            ### Query to find plate by ID
            plate = collection.find_one({'plateId': plate_id})
            
            ### Prepare output based on query result
            if plate is not None:
                return {'user': plate['userAccount'], 'campaign_id': plate['campaignId']}
            else:
                return None
                
        except Exception as e:
            ### Rethrow as RuntimeError to maintain existing function signature
            raise RuntimeError(f"Unexpected error occurred: {e}")
    ### FETCH_TAG find_user_from_plate_id

    ### FETCH_TAG find_last_fished_xtal
    def find_last_fished_xtal(self, user, campaign_id):
        """
        This function fetches the most recently 'fished' crystal for a given user and campaign.
        It queries the MongoDB collection 'wells' and sorts the results by the 'shifterTimeOfDeparture' field in descending order.
    
        :param user: The account of the user
        :param campaign_id: The ID of the campaign
        :return: A list of dictionaries containing the 'fished' crystals
        :rtype: list
        """
        ### Initialize the MongoDB collection
        collection = self.__get_collection('wells')
        
        ### Construct the query parameters
        query = {
            'userAccount': user,
            'campaignId': campaign_id,
            'fished': True
        }
        
        ### Execute the query and sort the results by 'shifterTimeOfDeparture' in descending order
        results = collection.find(query).sort('shifterTimeOfDeparture', pymongo.DESCENDING)
        
        ### Convert the query results to a list and return
        return list(results)
    ### FETCH_TAG find_last_fished_xtal

    ### FETCH_TAG get_next_xtal_number
    def get_next_xtal_number(self, plate_id):
        """
        Given a plate_id, the function finds the owner and campaign_id of the plate.
        It then looks for previously fished crystals and finds the last used xtal name.
        Finally, it parses the name to determine the next available number for a new crystal.
        
        :param plate_id: The plate identifier
        :return: An integer representing the next available crystal number
        :raises RuntimeError: If the user for the given plate cannot be found
        """
        ### Fetch user and campaign information based on plate_id
        user_info = self.find_user_from_plate_id(plate_id)
        
        if user_info is None:
            raise RuntimeError(f'Cannot find the user for plate: {plate_id}')
    
        user = user_info['user']
        campaign_id = user_info['campaign_id']
        
        ### Fetch the list of previously fished crystals for the user and campaign
        xtals = self.find_last_fished_xtal(user, campaign_id)
    
        ### Create a list of previously used xtal numbers
        xtals_list = []
        for xtal in xtals:
            xtal_name = xtal['xtalName']
            if xtal_name is None:  # Indicates that fishing was unsuccessful
                continue
            
            ### Assume xtal name is in the format "xtal-<number>" and extract the number
            xtal_number = int(xtal_name.split('-')[-1])
            
            xtals_list.append(xtal_number)
    
        ### Find the next available xtal number
        try:
            next_number = max(xtals_list) + 1
        except ValueError:  # This means xtals_list is empty, so we default to 1
            next_number = 1
        
        return next_number
    ### FETCH_TAG get_next_xtal_number

    ### FETCH_TAG is_crystal_already_fished
    def is_crystal_already_fished(self, plateId, wellId):
        """
        Checks if the crystal from a given plate and well is already fished.
    
        Parameters:
            plateId (str): ID of the plate.
            wellId (str): ID of the well.
    
        Returns:
            bool: True if the crystal is already fished, False otherwise.
        """
        ### Initialize MongoDB collection
        collection = self.__get_collection('wells')
    
        ### Build the query
        query = {'plateId': plateId, 'well': wellId}
    
        ### Execute the query
        query_result = collection.find(query)
    
        ### Convert query result to a list
        documents = list(query_result)
    
        ### Check if any document was returned
        if len(documents) > 0:
            ### Fetch the 'fished' field from the first document
            return documents[0]['fished']
    
        ### Return False if no document is found
        return False
    ### FETCH_TAG is_crystal_already_fished

    ### FETCH_TAG update_shifter_fishing_result
    def update_shifter_fishing_result(self, well_shifter_data, xtal_name_index, xtal_name_prefix='xtal'):
        """
        Update the fishing result based on the data received from the well shifter.
    
        Parameters:
        - well_shifter_data (dict): A dictionary with fishing result for given well.
        - xtal_name_index (int): An integer number that will be added to xtal_name_prefix.
        - xtal_name_prefix (str): A prefix used to create a crystal name.
    
        Returns:
        - pymongo.results.UpdateResult: The result of the MongoDB update operation.
        """
        plateId = well_shifter_data['plateId']
        wellId = well_shifter_data['plateRow'] + well_shifter_data['plateColumn'] + well_shifter_data['plateSubwell']
    
        ### Validate if the crystal is already fished
        if self.is_crystal_already_fished(plateId=plateId, wellId=wellId):
            return
    
        ### Sanitize data: Replace all empty fields with None
        for key, value in well_shifter_data.items():
            if value == '':
                well_shifter_data[key] = None
    
        ### Convert date strings to ISO Date format
        date_format = "%Y-%m-%d %H:%M:%S.%f"
        for date_field in ['timeOfArrival', 'timeOfDeparture']:
            if well_shifter_data[date_field]:
                try:
                    well_shifter_data[date_field] = datetime.datetime.strptime(well_shifter_data[date_field], date_format)
                except TypeError:
                    well_shifter_data[date_field] = None
    
        ### Parse the duration field
        if well_shifter_data['duration']:
            well_shifter_data['duration'] = parser.parse(well_shifter_data['duration'])
    
        ### Determine fishing status and crystal name based on shifter comment
        comment = well_shifter_data['comment']
        fished, xtal_name = False, None
        if comment:
            if comment.startswith('OK'):
                fished, xtal_name = True, f"{xtal_name_prefix}-{xtal_name_index}"
            elif comment.startswith('FAIL'):
                fished = True
    
        ### Prepare MongoDB query and execute the update
        query = {'plateId': plateId, 'well': wellId}
        update = {
            '$set': {
                'shifterComment': comment,
                'shifterXtalId': well_shifter_data['xtalId'],
                'shifterTimeOfArrival': well_shifter_data['timeOfArrival'],
                'shifterTimeOfDeparture': well_shifter_data['timeOfDeparture'],
                'shifterDuration': well_shifter_data['duration'],
                'puckBarcode': well_shifter_data['destinationName'],
                'puckPosition': well_shifter_data['destinationLocation'],
                'pinBarcode': well_shifter_data['barcode'],
                'puckType': well_shifter_data['externalComment'],
                'fished': fished,
                'xtalName': xtal_name
            }
        }
    
        collection = self.__get_collection('wells')
        return collection.update_one(query, update, False)
    ### FETCH_TAG update_shifter_fishing_result

    ### FETCH_TAG get_soaked_wells
    def get_soaked_wells(self, user, campaign_id):
        """
        Return list of wells that have been soaked but not yet fished.
        
        Parameters:
        - user (str): The user account identifier.
        - campaign_id (str): The identifier for the campaign.
        
        Returns:
        - list: A list of dictionaries, each representing a well that has been soaked but not yet fished.
        """
        # Get the MongoDB collection for 'wells'
        collection = self.__get_collection('wells')
    
        # Build the MongoDB query
        query = {
            'userAccount': user,
            'campaignId': campaign_id,
            'soakTransferTime': {'$ne': None},
            'fished': False,
            'soakTransferStatus': 'OK'
        }
    
        # Execute the query and fetch the result
        result = collection.find(query)
        
        return list(result)
    ### FETCH_TAG get_soaked_wells

    ### FETCH_TAG get_number_of_unsoaked_wells
    def get_number_of_unsoaked_wells(self, user, campaign_id):
        """
        Calculates the number of unsoaked wells a user has for a given campaign.
        :param user: The username to query.
        :param campaign_id: The campaign ID to query.
        :return: Integer representing the number of unsoaked wells.
        """
        
        ### Get the 'wells' collection from the database
        collection = self.__get_collection('wells')
        
        ### Define the query for finding unsoaked wells for the given user and campaign
        query = {'userAccount': user, 'campaignId': campaign_id, 'soakStatus': None}
        
        ### Use count_documents() to count the number of documents matching the query
        ### This replaces the deprecated find().count() method
        unsoaked_count = collection.count_documents(query)
        
        return unsoaked_count
    ### FETCH_TAG get_number_of_unsoaked_wells

    ### FETCH_TAG update_soaking_duration
    @send_notification('wells')
    def update_soaking_duration(self, user, campaign_id, wells):
        """
        Update the soakDuration field for wells by calculating the time elapsed since soakTransferTime.
        
        Parameters:
            user (str): The user making the request.
            campaign_id (str): The campaign ID associated with the wells.
            wells (list): List of dictionaries containing well data.
            
        Returns:
            dict: A dictionary containing MongoDB update result information.
        """
        collection = self.__get_collection('wells')
        current_time = datetime.datetime.now()
    
        for well in wells:
            well_id = well['_id']
            soak_start_time = well['soakTransferTime']
            elapsed_time = current_time - soak_start_time
            
            query = {'_id': well_id}
            update_operation = {'$set': {'soakDuration': elapsed_time.total_seconds()}}
            
            try:
                result = collection.update_one(query, update_operation)
            except Exception as e:
                print(f"Failed to update soaking duration for well {well_id}: {e}")
                return None
    
            # Mimic the old update result structure
            formatted_result = {'nModified': result.modified_count, 
                                'ok': 1.0 if result.acknowledged else 0.0, 
                                'n': result.matched_count}
        
        return formatted_result
    ### FETCH_TAG update_soaking_duration

    ### FETCH_TAG get_all_fished_wells
    def get_all_fished_wells(self, user, campaign_id):
        """
        Fetch all fished wells from the database for a given user and campaign ID.
        
        Parameters:
        - user: The user account
        - campaign_id: The campaign identifier
        
        Returns:
        - List of dictionaries representing all fished wells for the user and campaign.
        """
        ### Initialize MongoDB collection
        collection = self.__get_collection('wells')
        
        ### Define the query parameters
        query = {'userAccount': user, 'campaignId': campaign_id, 'fished': True}
        
        ### Execute the query
        result = collection.find(query)
        
        return list(result)
    ### FETCH_TAG get_all_fished_wells

    ### FETCH_TAG get_all_wells_not_exported_to_datacollection_xls
    def get_all_wells_not_exported_to_datacollection_xls(self, user, campaign_id):
        """
        Fetches all well data not exported to datacollection xls.
        Args:
            user (str): The user account.
            campaign_id (str): The campaign ID.
        Returns:
            list: A list of well data that meet the conditions.
        """
        collection = self.__get_collection('wells')
        query = {
            'userAccount': user,
            'campaignId': campaign_id,
            'fished': True,
            'exportedToXls': False,
            'xtalName': {'$ne': None}
        }
        try:
            result = collection.find(query)
            return list(result)
        except Exception as e:
            print(f"Error occurred while fetching wells: {e}")
            return []
    ### FETCH_TAG get_all_wells_not_exported_to_datacollection_xls

    ### FETCH_TAG mark_exported_to_xls
    def mark_exported_to_xls(self, wells):
        """
        Sets exportedToXls flag to True for documents in the 'wells' collection.
    
        :param wells: List of dictionaries, each containing well data.
        :return: A dictionary containing the last update result.
        """
        well_collection = self.__get_collection('wells')
        last_update_result = None  # Initialize last update result variable
    
        for well_data in wells:
            user_account = well_data['userAccount']
            campaign_id = well_data['campaignId']
            well_id_query = {'_id': well_data['_id']}
            update_action = {'$set': {'exportedToXls': True}}
    
            # Execute the update and capture the result
            update_result = well_collection.update_one(well_id_query, update_action)
    
            # Mimic the old update result structure for compatibility
            last_update_result = {
                'nModified': update_result.modified_count,
                'ok': 1.0 if update_result.acknowledged else 0.0,
                'n': update_result.matched_count
            }
    
        # Send notification
        self.send_notification(user_account, campaign_id, 'wells')
        return last_update_result
    ### FETCH_TAG mark_exported_to_xls

    ### FETCH_TAG send_notification
    def send_notification(self, user_account, campaign_id, notification_type):
        """
        Sends a notification by inserting a new document into the notifications collection.
        
        Parameters:
        - user_account (str): The account to which the notification is sent.
        - campaign_id (str): The ID of the campaign related to the notification.
        - notification_type (str): The type of the notification.
        
        Returns:
        InsertOneResult: The result of the MongoDB insert operation.
        """
        collection = self.__get_collection('notifications')
        doc = {
            'userAccount': user_account,
            'campaignId': campaign_id,
            'createdOn': datetime.datetime.now(),
            'notification_type': notification_type
        }
    
        r = collection.insert_one(doc)
        return r
    ### FETCH_TAG send_notification

    ###
    ### Group of methods sending notifications
    ###

    ### FETCH_TAG get_notifications
    def get_notifications(self, user_account, campaign_id, timestamp):
        """
        Fetches the notifications from the database for a given user account, campaign, and timestamp.
    
        Args:
            user_account (str): The user account to filter notifications for.
            campaign_id (str): The campaign ID to filter notifications for.
            timestamp (datetime): The starting timestamp for filtering notifications.
    
        Returns:
            list: List of dictionaries representing the notifications.
    
        Side-effects:
            - Converts the MongoDB ObjectIds to string format in the "_id" fields.
        """
        ### Find the notifications in the database and fetch them
        cursor = self._db.Notifications.find(
            {'userAccount': user_account, 'campaignId': campaign_id, 'createdOn': {'$gte': timestamp}},
            cursor_type=pymongo.CursorType.TAILABLE_AWAIT
        )
        notifications = list(cursor)
    
        ### Convert MongoDB ObjectIds to string format
        for notification in notifications:
            notification["_id"] = str(notification["_id"])
        return notifications
    ### FETCH_TAG get_notifications

    ### FETCH_TAG check_if_db_connected
    def check_if_db_connected(self):
        """
        There is no action required here as the utilities script is used by the database and does not need an inverse connection.
        
        Side-effects:
            - Prints a message stating that no action is needed for this function.
        """
        print("check_if_db_connected: No action required here. The DB connection is checked in ffcs_db_server.py check_if_db_connected.")
    ### FETCH_TAG check_if_db_connected

    ### FETCH_TAG_TEST test_dummy_01
    def test_dummy_01(self):
        print("test_dummy_01")
    ### FETCH_TAG_TEST test_dummy_01

    ### FETCH_TAG_TEST test_dummy_02
    def test_dummy_02(self):
        print("test_dummy_02")
    ### FETCH_TAG_TEST test_dummy_02

    ### FETCH_TAG_TEST test_dummy_03
    def test_dummy_03(self):
        print("test_dummy_03")
    ### FETCH_TAG_TEST test_dummy_03

    ### FETCH_TAG_TEST test_dummy_04
    def test_dummy_04(self):
        print("test_dummy_04")
    ### FETCH_TAG_TEST test_dummy_04

    ### FETCH_TAG_TEST test_dummy_05
    def test_dummy_05(self):
        print("test_dummy_05")
    ### FETCH_TAG_TEST test_dummy_05

