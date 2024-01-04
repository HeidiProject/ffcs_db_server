# Standard Libraries
from datetime import datetime
from typing import List, Optional, Dict, Any

# Third-Party Libraries
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient
import pymongo
import bson
from bson import ObjectId, Timestamp
from bson.json_util import dumps

# Your Libraries
from ffcs_db_utils import ffcs_db_utils, LibraryAlreadyImported

app = FastAPI()

### Pydantic base models
class Plate(BaseModel):
    userAccount: str
    plateId: str
    campaignId: str
    plateType: str = 'SwissCl'
    dropVolume: float
    batchId: Optional[str] = None
    createdOn: datetime = Field(default_factory=datetime.now)
    lastImaged: Optional[datetime] = None
    soakPlacesSelected: bool = False
    soakStatus: Optional[str] = None
    soakExportTime: Optional[datetime] = None
    soakTransferTime: Optional[datetime] = None
    cryoProtection: bool = False
    redesolveApplied: bool = False

class Well(BaseModel):
    userAccount: str
    campaignId: str
    plateId: str
    well: str
    wellEcho: str
    x: int
    y: int
    xEcho: float
    yEcho: float
    libraryAssigned: bool = False
    libraryName: Optional[str] = None
    libraryBarcode: Optional[str] = None
    libraryId: Optional[str] = None
    solventTest: bool = False
    sourceWell: Optional[str] = None
    smiles: Optional[str] = None
    compoundCode: Optional[str] = None
    libraryConcentration: Optional[float] = None
    solventVolume: Optional[float] = None
    ligandTransferVolume: Optional[float] = None
    ligandConcentration: Optional[float] = None
    soakStatus: Optional[str] = None
    soakExportTime: Optional[datetime] = None
    soakTransferTime: Optional[datetime] = None
    soakTransferStatus: Optional[str] = None
    cryoProtection: bool = False
    cryoDesiredConcentration: Optional[float] = None
    cryoTransferVolume: Optional[float] = None
    cryoSourceWell: Optional[str] = None
    cryoStatus: Optional[str] = None
    cryoExportTime: Optional[datetime] = None
    cryoTransferTime: Optional[datetime] = None
    cryoName: Optional[str] = None
    cryoBarcode: Optional[str] = None
    redesolveApplied: bool = False
    redesolveName: Optional[str] = None
    redesolveBarcode: Optional[str] = None
    redesolveSourceWell: Optional[str] = None
    redesolveTransferVolume: Optional[float] = None
    redesolveStatus: Optional[str] = None
    redesolveExportTime: Optional[datetime] = None
    shifterComment: Optional[str] = None
    shifterXtalId: Optional[str] = None
    shifterTimeOfArrival: Optional[datetime] = None
    shifterTimeOfDeparture: Optional[datetime] = None
    shifterDuration: Optional[float] = None
    puckBarcode: Optional[str] = None
    puckPosition: Optional[str] = None
    pinBarcode: Optional[str] = None
    puckType: Optional[str] = None
    fished: bool = False
    xtalName: Optional[str] = None
    soakDuration: Optional[float] = None
    notes: Optional[str] = None
    exportedToXls: bool = False

# Define your Pydantic model for a library
class CampaignLibrary(BaseModel):
    _id: str
    userAccount: str
    campaignId: str
    libraryName: str
    libraryBarcode: str
    fragments: list

class PlateResponse(BaseModel):
    acknowledged: bool
    inserted_id: str = None  # Optional field

class UpdateDocument(BaseModel):
    user_account: str = Field(...)
    campaign_id: str = Field(...)
    collection: str = Field(...)
    doc_id: str = Field(...)
    kwargs: Dict[str, str] = Field(...)

class UpdateNotesRequest(BaseModel):
    user: str
    campaign_id: str
    doc_id: str
    note: str

class MarkPlateDone(BaseModel):
    user_account: str = Field(..., example="user1")
    campaign_id: str = Field(..., example="campaign1")
    plate_id: str = Field(..., example="plate1")
    last_imaged: datetime
    batch_id: Optional[str] = Field(None, example="batch1")

class RedesolveRequest(BaseModel):
    user_account: str
    campaign_id: str
    target_plate: str
    target_well: str
    redesolve_transfer_volume: float
    redesolve_source_well: str
    redesolve_name: str
    redesolve_barcode: str

class UpdateShifterFishingResultRequest(BaseModel):
    well_shifter_data: dict
    xtal_name_index: int
    xtal_name_prefix: Optional[str] = 'xtal'

class UpdateResult(BaseModel):
    matched_count: int
    modified_count: int
    upserted_id: Optional[str]  # upserted_id can be None
    raw_result: dict

class UpdateSoakDurationData(BaseModel):
    user: str = Field(..., example="user1")
    campaign_id: str = Field(..., example="campaign1")
    wells: List[Dict[str, Any]]

class FragmentRequest(BaseModel):
    library: dict
    well_id: str
    fragment: dict
    solvent_volume: float
    ligand_transfer_volume: float
    ligand_concentration: float
    is_solvent_test: bool = False

class CampaignRequest(BaseModel):
    user: str
    campaign_id: str

class MarkExportedToXlsData(BaseModel):
    wells: List[Dict[str, Any]]

class ExportData(BaseModel):
    user: str
    campaign_id: str
    data: List[Well]

class PlateSoakExport(BaseModel):
    _id: str
    soakExportTime: datetime

# Define your Pydantic model for a library
class CampaignLibrary(BaseModel):
    _id: str
    userAccount: str
    campaignId: str
    libraryName: str
    libraryBarcode: str
    fragments: list

def serializable_update_result(result):
    # Define the serializable_raw_result based on the result object
    serializable_raw_result = {
        'n': result.modified_count,
        'nModified': result.modified_count,
        'ok': 1.0 if result.modified_count >= 1 else 0.0
    }

    # Create the UpdateResult object
    update_result = UpdateResult(
        matched_count=result.matched_count,
        modified_count=result.modified_count,
        upserted_id=result.upserted_id,
        raw_result=serializable_raw_result
    )

    return update_result

@app.on_event("startup")
async def startup_event():
    global client
    client = ffcs_db_utils()

@app.on_event("shutdown")
async def shutdown_event():
    global client
    client.close()

### FETCH_TAG delete_by_id
@app.delete("/delete_by_id/{collection}/{doc_id}")
async def delete_by_id(collection: str, doc_id: str):
    """Delete a document by its id"""
    result = client.delete_by_id(collection, doc_id)
    if result:
        return {"message": f"Document with id {doc_id} successfully deleted from {collection}",
                "acknowledged": result}
    else:
        raise HTTPException(status_code=404, detail="Document not found or delete operation was not acknowledged")
### FETCH_TAG delete_by_id

### FETCH_TAG delete_by_query
@app.post("/delete_by_query/{collection}")
async def delete_by_query(collection: str, query: dict):
    """Delete documents that match the provided query"""
    result = client.delete_by_query(collection, query)
    if result:
        return {"message": f"Documents matching the query {query} successfully deleted from {collection}",
                "acknowledged": result}
    else:
        raise HTTPException(status_code=404, detail="No documents matching the query found or delete operation was not acknowledged")
### FETCH_TAG delete_by_query

### FETCH_TAG check_if_db_connected
@app.get("/check_if_db_connected")
async def check_if_db_connected():
    """
    Checks the connection to the database.
    
    Returns:
        bool: True if the connection is successful, otherwise raises an HTTPException.

    Raises:
        HTTPException: If unable to connect to the database.
    """
    try:
        client._client.server_info()
    except (pymongo.errors.ServerSelectionTimeoutError,
            pymongo.errors.AutoReconnect,
            pymongo.errors.OperationFailure):
        ### Raise HTTP exception if connection fails
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    ### Connection successful
    return True
### FETCH_TAG check_if_db_connected

### FETCH_TAG get_collection
@app.get("/get_collection/{collection_name}")
async def get_collection(collection_name: str):
    """
    COMMENT (by ChatGPT):
    If you don't want to modify ffcsdbclient and want to adhere to the principle of encapsulation,
    you have limited options since directly calling private methods from outside the class is generally discouraged.
    
    However, there is a way in Python to access these private methods but it's generally discouraged
    and it's considered a bad practice because it violates the principle of encapsulation.
    Python mangles the name of the method with double underscore prefix by adding a _classname before it.
    So you could technically access it like so:
    """
    global client
    # Using Python name mangling to access private method
    collection = client._ffcs_db_utils__get_collection(collection_name)
    return {"collection": str(collection)}
### FETCH_TAG get_collection

### FETCH_TAG get_libraries
@app.get("/get_libraries/")
async def get_libraries() -> List[dict]:
    """
    FastAPI endpoint to retrieve a list of all libraries from the database.

    This endpoint calls the get_libraries method from the client to fetch all library records. Each record's
    '_id' and 'libraryBarcode', which are MongoDB ObjectIds, are converted to strings for JSON compatibility.
    If an error occurs during this process, an HTTPException is raised with the appropriate error details.

    Returns:
        List[dict]: A list of dictionaries, each representing a library, with ObjectIds converted to strings.

    Raises:
        HTTPException: If an exception occurs while fetching the libraries or processing the data.
    """
    try:
        libraries = client.get_libraries()
        for library in libraries:
            library['_id'] = str(library['_id'])  # Convert the ObjectId to string for each library
            if 'libraryBarcode' in library:
                library['libraryBarcode'] = str(library['libraryBarcode'])  # Convert the ObjectId to string for each library
        return libraries
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_libraries

### FETCH_TAG get_plate
@app.get("/get_plate/{user_account}/{campaign_id}/{plate_id}")
async def get_plate(user_account: str, campaign_id: str, plate_id: str):
    plate = client.get_plate(user_account, campaign_id, plate_id)
    if plate is not None:
        plate['_id'] = str(plate['_id'])
    return plate
### FETCH_TAG get_plate

### FETCH_TAG get_plates
@app.get("/get_plates/{user_account}/{campaign_id}")
async def get_plates(user_account: str, campaign_id: str):
    plates_cursor = client.get_plates(user_account, campaign_id)
    plates_list = list(plates_cursor)  # Converts the Cursor to a list
    for plate in plates_list:
        plate['_id'] = str(plate['_id'])
    return plates_list
### FETCH_TAG get_plates

### FETCH_TAG get_campaigns
@app.get("/get_campaigns/{user_account}")
async def get_campaigns(user_account: str):
    campaigns_cursor = client.get_campaigns(user_account)
    campaigns_list = list(campaigns_cursor)
    return campaigns_list
### FETCH_TAG get_campaigns

### FETCH_TAG add_plate
@app.post("/add_plate/", response_model=PlateResponse)
async def add_plate(plate: Plate):
    result = client.add_plate(plate.dict())
    return {
        "acknowledged": result.acknowledged,
        "inserted_id": str(result.inserted_id) if result.acknowledged else None
    }
### FETCH_TAG add_plate

### FETCH_TAG add_well
@app.post("/add_well/")
async def add_well(well: Well):
    try:
        result = client.add_well(well.dict())
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Prepare the response
    response = {
        "acknowledged": result.acknowledged,
        "inserted_id": str(result.inserted_id) if result.acknowledged else None
    }

    return response
### FETCH_TAG add_well

### FETCH_TAG add_campaign_library
@app.post("/add_campaign_library/")
async def add_campaign_library(campaign_library: CampaignLibrary):
    try:
        result = client.add_campaign_library(campaign_library.dict())
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Prepare the response
    response = {
        "acknowledged": result.acknowledged,
        "inserted_id": str(result.inserted_id) if result.acknowledged else None
    }

    return response
### FETCH_TAG add_campaign_library

### FETCH_TAG insert_campaign_library
@app.post("/insert_campaign_library/")
async def insert_campaign_library(campaign_library: CampaignLibrary) -> dict:
    """
    FastAPI endpoint to insert a new campaign library into the database.

    This endpoint receives a campaign library as input, attempts to insert it into the database, 
    and returns the result of the operation. The input is expected to be a valid CampaignLibrary object.
    If the insertion is successful, it returns a dictionary with 'acknowledged' set to True and the 'inserted_id' of the new library.
    In case of a RuntimeError, it raises an HTTPException with the appropriate details.

    Args:
        campaign_library (CampaignLibrary): An object representing the campaign library to be inserted.

    Returns:
        dict: A dictionary containing the 'acknowledged' status and 'inserted_id' of the inserted library.

    Raises:
        HTTPException: If a RuntimeError occurs during the insertion process.
    """
    try:
        result = client.insert_campaign_library(campaign_library.dict())
        return {
            "acknowledged": result.acknowledged,
            "inserted_id": str(result.inserted_id) if result.acknowledged else None
        }
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG insert_campaign_library

### FETCH_TAG add_wells
@app.post("/add_wells/")
async def add_wells(wells: List[Well]):
    result = client.add_wells([well.dict() for well in wells])
### FETCH_TAG add_wells

### FETCH_TAG update_by_object_id
@app.put("/update_by_object_id")
async def update_by_object_id(update_doc: UpdateDocument):
    try:
        # Access the private method get_collection
        ###collection = client._ffcsdbclient__get_collection(update_doc.collection.lower())
        collection = client._ffcs_db_utils__get_collection(update_doc.collection.lower())
        # Check if doc_id is a valid ObjectId
        if not bson.objectid.ObjectId.is_valid(update_doc.doc_id):
            raise HTTPException(status_code=400, detail="doc_id must be a valid ObjectId")

        doc_id = bson.objectid.ObjectId(update_doc.doc_id)
        query = {'userAccount': update_doc.user_account, 'campaignId': update_doc.campaign_id, 'collection': update_doc.collection, '_id': doc_id}
        update = {'$set': update_doc.kwargs}
        result = client.update_by_object_id(update_doc.user_account, update_doc.campaign_id, update_doc.collection, doc_id, **update_doc.kwargs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    result_data = {
        "acknowledged": result.acknowledged,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
        "upserted_id": str(result.upserted_id) if result.upserted_id else None,
    }

    return {"result": result_data}
### FETCH_TAG update_by_object_id

### FETCH_TAG update_by_object_id_NEW
@app.put("/update_by_object_id_NEW")
async def update_by_object_id_NEW(update_doc: UpdateDocument):
    """
    Update a document in the database by its ObjectId.
    (WORK IN PROGESS; UNUSED FUNCTION)

    Parameters:
    - update_doc (UpdateDocument): The document to be updated.

    Returns:
    dict: A dictionary containing the result of the update operation.

    Raises:
    HTTPException: If an error occurs during the update operation.
    """
    try:
        # Access the private method get_collection
        collection = client._ffcsdbclient__get_collection(update_doc.collection.lower())
        # Check if doc_id is a valid ObjectId
        if not bson.objectid.ObjectId.is_valid(update_doc.doc_id):
            raise HTTPException(status_code=400, detail="doc_id must be a valid ObjectId")

        doc_id = bson.objectid.ObjectId(update_doc.doc_id)
        query = {'userAccount': update_doc.user_account, 'campaignId': update_doc.campaign_id, 'collection': update_doc.collection, '_id': doc_id}
        update = {'$set': update_doc.kwargs}
        result = client.update_by_object_id_NEW(update_doc.user_account, update_doc.campaign_id, update_doc.collection, doc_id, **update_doc.kwargs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    """
    result_data = {
        "nModified": result.nModified,
        "ok": result.ok,
        "n": result.n,
    }
    """

    return {"Result": result}
### FETCH_TAG update_by_object_id_NEW

### FETCH_TAG is_plate_in_database
@app.get("/is_plate_in_database/{plate_id}")
async def is_plate_in_database(plate_id: str):
    result = client.is_plate_in_database(plate_id)
    return {"exists": result}
### FETCH_TAG is_plate_in_database

### FETCH_TAG get_unselected_plates
@app.get("/get_unselected_plates/{user_account}")
async def get_unselected_plates(user_account: str):
    try:
        # Get the plates data
        plates_data = client.get_unselected_plates(user_account)

        # Convert the ObjectId to string
        for item in plates_data:
            item["_id"] = str(item["_id"])

        return plates_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
### FETCH_TAG get_unselected_plates

### FETCH_TAG mark_plate_done
@app.put("/mark_plate_done")
async def mark_plate_done(data: MarkPlateDone):
    result = client.mark_plate_done(
        data.user_account,
        data.campaign_id,
        data.plate_id,
        data.last_imaged,
        data.batch_id
    )
    return {"Result": result}
### FETCH_TAG mark_plate_done

### FETCH_TAG get_all_wells
@app.get("/get_all_wells/")
async def get_all_wells(user_account: str, campaign_id: Optional[str] = None):
    try:
        wells = client.get_all_wells(user_account, campaign_id)
        return wells
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_all_wells

### FETCH_TAG get_wells_from_plate
@app.get("/get_wells_from_plate/")
async def get_wells_from_plate(user_account: str, campaign_id: str, plate_id: str, kwargs: Optional[Dict[str, Any]] = {}):
    try:
        wells = client.get_wells_from_plate(user_account, campaign_id, plate_id, **kwargs)

        # Convert the ObjectId to string
        for well in wells:
            well["_id"] = str(well["_id"])
            # Only convert if libraryId is not None
            if well["libraryId"] is not None:
                well["libraryId"] = str(well["libraryId"])  # Convert the ObjectId to string

        return wells
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_wells_from_plate

### FETCH_TAG get_one_well
@app.get("/get_one_well/")
async def get_one_well(well_id: str):
    try:
        well = client.get_one_well(ObjectId(well_id))
        well["_id"] = str(well["_id"])  # Convert the ObjectId to string

        # Only convert if libraryId is not None
        if well["libraryId"] is not None:
            well["libraryId"] = str(well["libraryId"])  # Convert the ObjectId to string

        return well
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_one_well

### FETCH_TAG get_one_campaign_library
@app.get("/get_one_campaign_library/")
async def get_one_campaign_library(library_id: str):
    try:
        library = client.get_one_campaign_library(ObjectId(library_id))
        library["_id"] = str(library["_id"])  # Convert the ObjectId to string
        return library
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_one_campaign_library

### FETCH_TAG get_one_library
@app.get("/get_one_library/")
async def get_one_library(library_id: str) -> dict:
    """
    FastAPI endpoint to retrieve a single library record from the database.

    This endpoint accepts a string representation of the library's ObjectId and queries the database
    to retrieve the corresponding library record. The record's ObjectId fields ('_id' and 'libraryBarcode')
    are converted to string format for JSON compatibility. In case of any errors, an HTTPException is raised.

    Args:
        library_id (str): The string representation of the library's ObjectId.

    Returns:
        dict: A dictionary containing the library record with ObjectId fields converted to strings.

    Raises:
        HTTPException: If an error occurs during the retrieval process.
    """
    try:
        # Convert string to ObjectId
        library = client.get_one_library(ObjectId(library_id))
        if library:
            # Convert ObjectId fields to strings
            library["_id"] = str(library["_id"])
            if "libraryBarcode" in library:
                library["libraryBarcode"] = str(library["libraryBarcode"])
            return library
        else:
            raise HTTPException(status_code=404, detail="Library not found")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

### FETCH_TAG get_one_library

### FETCH_TAG get_smiles
@app.get("/get_smiles/")
async def get_smiles(user_account: str, campaign_id: str, xtal_name: str) -> dict:
    """
    FastAPI endpoint to retrieve the SMILES string for a specific crystal associated with a user account and campaign.

    The endpoint accepts the user account, campaign ID, and crystal name as parameters and uses them to query
    the database for the corresponding SMILES string. It returns the SMILES string if found, otherwise returns None.

    Args:
        user_account (str): The identifier of the user account.
        campaign_id (str): The identifier of the campaign.
        xtal_name (str): The name of the crystal.

    Returns:
        dict: A dictionary containing the 'smiles' key with the SMILES string or None as its value.

    Raises:
        HTTPException: If an error occurs during the retrieval process, with a status code of 400 and the error detail.
    """
    try:
        smiles = client.get_smiles(user_account, campaign_id, xtal_name)
        return {"smiles": smiles}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_smiles

### FETCH_TAG get_not_matched_wells
@app.get("/get_not_matched_wells/")
async def get_not_matched_wells(user_account: str, campaign_id: str):
    """
    FastAPI endpoint to retrieve wells that are not matched based on specific criteria from the database. 
    The endpoint filters wells by user account and campaign ID, focusing on wells where 'compoundCode' is 
    None and 'cryoProtection' status meets certain conditions.

    Args:
        user_account (str): The user account identifier.
        campaign_id (str): The campaign identifier.

    Returns:
        List[dict]: A JSON response containing a list of dictionaries, each representing a well that matches 
                    the query criteria. The '_id' field of each well is converted to a string for JSON compatibility.

    Raises:
        HTTPException: If there is an issue with database access, query execution, or data transformation, 
                       it raises an HTTPException with status code 400.
    """
    try:
        wells = client.get_not_matched_wells(user_account, campaign_id)

        # Convert the ObjectId to string for each well
        for well in wells:
            well["_id"] = str(well["_id"])

        return wells
    except Exception as e:
        # Handle any exceptions and provide appropriate feedback via HTTP response
        raise HTTPException(status_code=400, detail=f"Failed to retrieve not matched wells: {e}")
### FETCH_TAG get_not_matched_wells

### FETCH_TAG get_id_of_plates_to_soak
@app.get("/get_id_of_plates_to_soak/")
async def get_id_of_plates_to_soak(user_account: str, campaign_id: str):
    """
    FastAPI endpoint to retrieve a list of plate IDs along with the count of wells 
    with and without an assigned library for each plate, filtered by user account 
    and campaign ID.

    This endpoint invokes a function in the utilities script that performs a MongoDB 
    aggregation to find plates matching the given user account and campaign ID. 
    It then aggregates data to count total wells, wells with a library assigned, 
    and wells without a library assigned for each plate.

    Args:
        user_account (str): The user account identifier.
        campaign_id (str): The campaign identifier.

    Returns:
        JSONResponse: A JSON response containing a list of dictionaries, each 
        containing the plate ID, total well count, count of wells with a library, 
        and count of wells without a library.

    Raises:
        HTTPException: If there is an issue with retrieving data, such as a database 
        access error, it raises an HTTPException with status code 400.
    """
    try:
        plates = client.get_id_of_plates_to_soak(user_account, campaign_id)
        return plates
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_id_of_plates_to_soak

### FETCH_TAG get_id_of_plates_to_cryo_soak
@app.get("/get_id_of_plates_to_cryo_soak/")
async def get_id_of_plates_to_cryo_soak(user_account: str, campaign_id: str):
    """
    FastAPI endpoint to retrieve a list of plate IDs along with the count of wells with 
    and without cryo protection for each plate, filtered by user account and campaign ID.
    
    This endpoint invokes a function in the utilities script that performs a MongoDB 
    aggregation to find plates matching the specified user account and campaign ID, 
    and then counts the number of wells with and without cryo protection for each plate.
    
    Args:
        user_account (str): The user account identifier.
        campaign_id (str): The campaign identifier.

    Returns:
        JSONResponse: A JSON response containing a list of dictionaries, each dictionary
        contains the plate ID, total well count, count of wells with cryo protection, 
        and count of wells without cryo protection.

    Raises:
        HTTPException: If there is an issue with retrieving data, such as a database 
        access error, it raises an HTTPException with status code 400.
    """
    try:
        plates = client.get_id_of_plates_to_cryo_soak(user_account, campaign_id)
        return plates
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_id_of_plates_to_cryo_soak

### FETCH_TAG get_id_of_plates_for_redesolve
@app.get("/get_id_of_plates_for_redesolve/")
async def get_id_of_plates_for_redesolve(user_account: str, campaign_id: str):
    """
    Endpoint to retrieve the IDs of plates for redesolve operation, along with the count 
    of wells with and without new solvent for each plate, filtered by user account and 
    campaign ID.

    This endpoint invokes a function that queries a MongoDB collection to find plates 
    matching the given user account and campaign ID. It then aggregates data to count 
    total wells, wells with new solvent, and wells without new solvent for each plate.

    Args:
        user_account (str): The user account identifier.
        campaign_id (str): The campaign identifier.

    Returns:
        JSONResponse: A JSON response containing a list of dictionaries, each dictionary 
        contains the plate ID, total well count, count of wells with new solvent, 
        and count of wells without new solvent.

    Raises:
        HTTPException: If there is an issue with retrieving data, such as a database 
        access error, it raises an HTTPException with status code 400.
    """
    try:
        plates = client.get_id_of_plates_for_redesolve(user_account, campaign_id)
        return plates
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_id_of_plates_for_redesolve

### FETCH_TAG export_to_soak_selected_wells
@app.post("/export_to_soak_selected_wells/")
async def export_to_soak_selected_wells(export_data: ExportData) -> Dict[str, Any]:
    """
    Endpoint to handle the export of soak data for selected wells.

    This endpoint receives a request containing user information and a list of wells,
    and triggers an update operation to set 'soakExportTime' and change 'soakStatus'
    to 'exported' for each well.

    Args:
        export_data (ExportData): An object containing 'user', 'campaign_id', and 'data' fields,
                                  where 'data' is a list of well dictionaries to be updated.

    Returns:
        Dict[str, Any]: A dictionary with the operation result, typically None.

    Raises:
        HTTPException: If a runtime error occurs during the process.
    """
    try:
        # Convert the list of well objects to dictionaries for the update operation
        well_data_dicts = [well.dict() for well in export_data.data]
        # Invoke the utility function and pass the converted data
        result = client.export_to_soak_selected_wells(export_data.user, export_data.campaign_id, well_data_dicts)
        return {"result": result}
    except RuntimeError as e:
        # Convert RuntimeError to HTTPException to provide a proper HTTP error response
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_to_soak_selected_wells

### FETCH_TAG export_cryo_to_soak_selected_wells
@app.post("/export_cryo_to_soak_selected_wells/")
async def export_cryo_to_soak_selected_wells(export_data: ExportData) -> Dict[str, Any]:
    """
    Endpoint to handle the export of cryopreservation data for selected wells.

    This endpoint receives a request containing user information and a list of wells, 
    and triggers an update operation to set 'cryoExportTime' and change 'cryoStatus' 
    to 'exported' for each well.

    Args:
        export_data: An instance of ExportData containing the user's information and 
                     the list of wells to be updated.

    Returns:
        A dictionary with the key 'result' indicating the success or failure of the operation.

    Raises:
        HTTPException: If a runtime error occurs during the process.
    """
    try:
        # The request's body will be converted to a dictionary and passed as arguments.
        result = client.export_cryo_to_soak_selected_wells(**export_data.dict())
        return {"result": result}
    except RuntimeError as e:
        # If an exception occurs, it is caught and an HTTPException is raised with the error details.
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_cryo_to_soak_selected_wells

### FETCH_TAG export_redesolve_to_soak_selected_wells
@app.post("/export_redesolve_to_soak_selected_wells/")
async def export_redesolve_to_soak_selected_wells(export_data: ExportData):
    """
    Endpoint to export 'redesolve' data to soak selected wells.

    This function triggers the export of 'redesolve' data to the selected wells,
    updating the 'redesolveExportTime' and 'redesolveStatus' fields in the database
    for the wells associated with the given user and campaign.

    Args:
        export_data (ExportData): An object containing 'user', 'campaign_id', and 'data' fields,
                                  where 'data' is a list of well dictionaries to be updated.

    Returns:
        dict: A dictionary with the operation result, typically None.

    Raises:
        HTTPException: An error with detailed message if the database update fails.
    """
    try:
        # Converting the well objects to dictionaries before passing to the utility function
        well_data_dicts = [well.dict() for well in export_data.data]
        result = client.export_redesolve_to_soak_selected_wells(export_data.user, export_data.campaign_id, well_data_dicts)
        return {"result": result}
    except RuntimeError as e:
        # Convert RuntimeError to HTTPException to provide proper HTTP error response
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_redesolve_to_soak_selected_wells

### FETCH_TAG export_to_soak
@app.post("/export_to_soak/")
async def export_to_soak(data: List[Dict[str, Any]]):
    """
    Processes export requests to update soak times for wells and plates in the database.

    Args:
        data: A list of dictionaries where each dictionary contains 'plateId' and 'soak_time'.
              The 'soak_time' should be in ISO format as a string.
              Example: [{'_id': 'plateId', 'soak_time': '2023-01-01T00:00:00'}]

    Returns:
        pymongo.results.UpdateResult: The result of the update operation from the database.

    Raises:
        HTTPException: If there's an error during the update process with a status code of 400.
    """
    try:
        # Convert 'soak_time' from ISO format string to datetime objects
        for item in data:
            item['soak_time'] = datetime.strptime(item['soak_time'], '%Y-%m-%dT%H:%M:%S.%f')

        # Call the client function to perform the update operation and get the result
        result = client.export_to_soak(data)

        # Assuming 'serializable_update_result' is a function that serializes the update result
        update_result = serializable_update_result(result)

        return update_result
    except RuntimeError as e:
        # Raise an HTTPException with the error detail if a runtime error occurs
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_to_soak

### FETCH_TAG export_redesolve_to_soak
@app.post("/export_redesolve_to_soak/")
async def export_redesolve_to_soak(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Endpoint that processes the 'export_redesolve_to_soak' request to update soak times.

    Receives a list of dictionaries containing well and plate data, parses the soak times,
    and forwards them to the client service to update the database. The response includes
    the count of matched and modified documents along with other operation details.

    Args:
        data: A list of dictionaries with keys '_id' and 'soak_time', where 'soak_time' is
              a string in ISO format that indicates the time of soaking.

    Returns:
        A dictionary containing the update operation result with keys 'matched_count',
        'modified_count', 'upserted_id', and 'raw_result'.

    Raises:
        HTTPException: An error response with status code 400 and detail of the exception
                       if there is a failure in processing the request or updating the database.
    """
    try:
        # Parse the soak times to datetime objects
        for item in data:
            item['soak_time'] = datetime.strptime(item['soak_time'], '%Y-%m-%dT%H:%M:%S.%f')

        # Call the utility function and get the result
        result = client.export_redesolve_to_soak(data)
        # Convert the result to a serializable format
        update_result = serializable_update_result(result)

        return update_result
    except RuntimeError as e:
        # If a runtime error occurs, raise an HTTPException with the error details
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_redesolve_to_soak

### FETCH_TAG export_cryo_to_soak
@app.post("/export_cryo_to_soak/")
async def export_cryo_to_soak(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Receives cryopreservation export requests and processes them to update the soak times of wells and plates.

    The soak times are parsed and sent to the client service which performs the update in the database.
    This endpoint returns the results of the update operation, including the number of matched and modified documents.

    Args:
        data (List[Dict[str, Any]]): The list of plates to be updated with their new soak times.
                                     Each dict must contain '_id' and 'soak_time' keys.

    Returns:
        Dict[str, Any]: A dictionary containing the results of the update operation.

    Raises:
        HTTPException: If a RuntimeError occurs during the processing of the request.
    """
    try:
        # Convert the 'soak_time' from ISO format string to datetime objects
        for item in data:
            item['soak_time'] = datetime.strptime(item['soak_time'], '%Y-%m-%dT%H:%M:%S.%f')

        # Call to the utility function from the client to update the database collections
        result = client.export_cryo_to_soak(data)
        update_result = serializable_update_result(result)

        return update_result
    except RuntimeError as e:
        # If there is an issue with the update operation, raise an HTTPException
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG export_cryo_to_soak

### FETCH_TAG import_soaking_results
@app.post("/import_soaking_results/")
async def import_soaking_results(wells_data: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Endpoint to import soaking results, updates SoakStatus of wells to 'Done'.

    Accepts a list of dictionaries containing well data and invokes the
    'import_soaking_results' method from the client to process and update the
    soak status in the database. Upon successful completion, returns a
    confirmation message.

    Args:
        wells_data (List[Dict[str, Any]]): A list of dictionaries, each containing data for
                                           a well, including 'plateId', 'wellEcho', and
                                           'transferStatus'.

    Returns:
        Dict[str, str]: A dictionary with the result message indicating successful import.

    Raises:
        HTTPException: An exception with status code 400 is raised if there is a runtime
                       error during the import process, with the error message provided by
                       the raised exception.
    """
    try:
        client.import_soaking_results(wells_data)
        return {"result": "Soaking results imported successfully."}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG import_soaking_results

### FETCH_TAG mark_soak_for_well_in_echo_done
@app.post("/mark_soak_for_well_in_echo_done/")
async def mark_soak_for_well_in_echo_done(data: Dict[str, Any]):
    """
    Endpoint to mark a well's soak status as 'done' after a transfer in Echo is completed.
    The endpoint expects a dictionary containing details about the well and the transfer status.

    Args:
        data (Dict[str, Any]): A dictionary containing 'user', 'campaign_id', 'plate_id', 
        'well_echo', and 'transfer_status' to update the soak status of a well.

    Returns:
        Dict[str, Any]: A dictionary with the update operation result, which includes the number
        of documents matched and modified, as well as other details of the operation.

    Raises:
        HTTPException: If an error occurs during the process, it captures and returns an HTTPException
        with status code 400 and the error detail.
    """
    try:
        user = data['user']
        campaign_id = data['campaign_id']
        plate_id = data['plate_id']
        well_echo = data['well_echo']
        transfer_status = data['transfer_status']

        # Call the utility client to update the soak status
        result = client.mark_soak_for_well_in_echo_done(
            user=user,
            campaign_id=campaign_id,
            plate_id=plate_id,
            well_echo=well_echo,
            transfer_status=transfer_status
        )
        
        # Convert the update result to a serializable format
        update_result = serializable_update_result(result)
        return update_result

    except Exception as e:
        # Re-raising as an HTTPException for the FastAPI framework to handle
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG mark_soak_for_well_in_echo_done

### FETCH_TAG add_cryo
@app.post("/add_cryo/")
async def add_cryo(data: Dict[str, Any]):
    """
    Handles the POST request to add cryoprotection details to a well.

    Args:
        data (Dict[str, Any]): A dictionary with keys 'user_account', 'campaign_id',
                               'target_plate', 'target_well', 'cryo_desired_concentration',
                               'cryo_transfer_volume', 'cryo_source_well', 'cryo_name', and
                               'cryo_barcode', representing the cryoprotection details.

    Returns:
        Dict[str, Any]: A dictionary containing the result of the update operation
                        with keys 'matched_count', 'modified_count', 'upserted_id',
                        and 'raw_result'.

    Raises:
        HTTPException: An exception with a status code 400 if there's a runtime error.
    """
    try:
        user_account = data['user_account']
        campaign_id = data['campaign_id']
        target_plate = data['target_plate']
        target_well = data['target_well']
        cryo_desired_concentration = data['cryo_desired_concentration']
        cryo_transfer_volume = data['cryo_transfer_volume']
        cryo_source_well = data['cryo_source_well']
        cryo_name = data['cryo_name']
        cryo_barcode = data['cryo_barcode']

        # Call utility function to add cryoprotection details to the database
        result = client.add_cryo(user_account, campaign_id, target_plate, target_well,
                                 cryo_desired_concentration, cryo_transfer_volume,
                                 cryo_source_well, cryo_name, cryo_barcode)
        
        # Convert the update result to a serializable format
        update_result = serializable_update_result(result)

        return update_result
    except RuntimeError as e:
        # Handle runtime errors during the cryo update process
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG add_cryo


### FETCH_TAG remove_cryo_from_well
@app.patch("/remove_cryo_from_well/{well_id}")
async def remove_cryo_from_well(well_id: str):
    """
    Endpoint to remove cryoprotectant data from a specified well by its ID.

    The function attempts to update the well document in the MongoDB database, setting cryo-related
    fields to None and cryoProtection to False. The updated result is returned in a serializable format.

    Args:
        well_id (str): The string representation of the well's ObjectId.

    Returns:
        dict: A dictionary containing the update operation results such as matched count, modified count, etc.

    Raises:
        HTTPException: An error occurred during the update process, returns a 400 status code with error details.
    """
    try:
        # Convert the string ID to an ObjectId and attempt to remove the cryo information.
        result = client.remove_cryo_from_well(ObjectId(well_id))
        # Serialize the update result to make it JSON serializable.
        update_result = serializable_update_result(result)

        return update_result
    except Exception as e:  # Catch any exception, which is more generic than RuntimeError.
        # Return an HTTPException with status code 400, which indicates a client error.
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG remove_cryo_from_well

### FETCH_TAG remove_new_solvent_from_well
@app.patch("/remove_new_solvent_from_well/{well_id}")
async def remove_new_solvent_from_well(well_id: str) -> Any:
    """
    Endpoint to remove the New Solvent (redissolve option) from a specified well by its ID.
    
    It receives the well ID as a string, converts it to an ObjectId, and uses the
    client's method to remove the redissolve option from the specified well. The result is
    converted to a serializable format before being returned.

    Args:
        well_id (str): The string representation of the unique identifier for the well 
                       from which the New Solvent should be removed.

    Returns:
        dict: A dictionary representing the outcome of the update operation, including the
              count of matched and modified documents.

    Raises:
        HTTPException: If the 'well_id' is not valid or if the update operation fails.
    """
    try:
        # Convert the well_id from a string to an ObjectId
        well_object_id = ObjectId(well_id)
        # Perform the removal of the new solvent using the utility function
        result = client.remove_new_solvent_from_well(well_object_id)
        # Serialize the update result to a dictionary
        update_result = serializable_update_result(result)
        return update_result
    except (TypeError, PyMongoError) as e:
        # Raise an HTTPException if there's a problem with the database operation
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG remove_new_solvent_from_well

### FETCH_TAG get_cryo_usage
@app.get("/get_cryo_usage/{user}/{campaign_id}")
async def get_cryo_usage(user: str, campaign_id: str):
    """Fetch the usage of cryo protection for wells belonging to a particular user and campaign.

    Args:
        user (str): The user account for which the query should be performed.
        campaign_id (str): The campaign identifier for which the query should be performed.

    Returns:
        list: A list of dictionaries containing information on cryo usage per source well.

    Raises:
        HTTPException: An exception is raised if the function encounters a runtime error, returning a 400 status code.
    """
    try:
        ### Execute get_cryo_usage function from client object and store result
        cryo_usage_result = client.get_cryo_usage(user, campaign_id)

        ### Return the result as is, since it's expected to be in JSON-compatible format
        return cryo_usage_result

    except RuntimeError as runtime_err:
        ### Raise an HTTPException with a 400 status code in case of a RuntimeError
        raise HTTPException(status_code=400, detail=str(runtime_err))
### FETCH_TAG get_cryo_usage

### FETCH_TAG get_solvent_usage
@app.get("/get_solvent_usage/{user}/{campaign_id}")
async def get_solvent_usage(user: str, campaign_id: str):
    """
    Fetch the solvent usage for a given user and campaign ID.
    
    Parameters:
        user (str): The user account identifier.
        campaign_id (str): The campaign identifier.
        
    Returns:
        list[dict]: A list of dictionaries, each containing information about solvent usage.
        
    Raises:
        HTTPException: An error occurred while fetching solvent usage.
    """
    
    try:
        ### Call the get_solvent_usage function from the client and store the result
        solvent_usage_result = client.get_solvent_usage(user, campaign_id)
        
        ### Return the solvent usage information
        return solvent_usage_result
        
    except RuntimeError as runtime_error:
        ### Raise HTTP 400 Bad Request if RuntimeError occurs
        raise HTTPException(status_code=400, detail=str(runtime_error))
        
### FETCH_TAG get_solvent_usage

### FETCH_TAG redesolve_in_new_solvent
@app.patch("/redesolve_in_new_solvent/")
async def redesolve_in_new_solvent(request: RedesolveRequest):
    """
    This FastAPI endpoint allows updating specific well information for the purpose of re-dissolving 
    samples in a new solvent. The function takes a request object that contains all the required fields 
    and updates the corresponding records in the database.
    
    Parameters:
        request: RedesolveRequest
            A request object containing user_account, campaign_id, target_plate, target_well, redesolve_transfer_volume, 
            redesolve_source_well, redesolve_name, and redesolve_barcode.
    
    Returns:
        JSON response with update results.
    
    Exceptions:
        HTTPException: If any errors occur during the operation, an HTTPException is raised with status_code 400.
    """
    
    try:
        ### Extract values from the request object
        user_account = request.user_account
        campaign_id = request.campaign_id
        target_plate = request.target_plate
        target_well = request.target_well
        redesolve_transfer_volume = request.redesolve_transfer_volume
        redesolve_source_well = request.redesolve_source_well
        redesolve_name = request.redesolve_name
        redesolve_barcode = request.redesolve_barcode
        
        ### Call the utility function to update the database
        result = client.redesolve_in_new_solvent(
            user_account, 
            campaign_id, 
            target_plate, 
            target_well, 
            redesolve_transfer_volume, 
            redesolve_source_well, 
            redesolve_name, 
            redesolve_barcode
        )
        
        ### Serialize the database update result to a JSON-serializable format
        update_result = serializable_update_result(result)
        
        ### Return the serialized result
        return update_result

    except RuntimeError as e:
        ### Log the error and raise an HTTP Exception with a 400 status code
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG redesolve_in_new_solvent

### FETCH_TAG update_notes
@app.patch("/update_notes/")
async def update_notes(request: UpdateNotesRequest):
    """
    Update notes for a given well identified by 'doc_id'.

    This FastAPI endpoint receives an UpdateNotesRequest object, which
    includes the user, campaign ID, document ID (doc_id), and the new note.
    It then updates the note for the specified well in the database and returns
    the update operation result.

    Args:
        request: An instance of UpdateNotesRequest containing the required fields
        for the update operation.

    Returns:
        A dictionary containing the raw result of the update operation.

    Raises:
        HTTPException: An exception is raised if the operation encounters
        a runtime error, and it returns a 400 status code along with
        the error message.
    """
    try:
        ### Execute the update_notes function from the client and get the result
        update_operation_result = client.update_notes(
            request.user, request.campaign_id, request.doc_id, request.note)
        
        ### Serialize the update result to make it JSON compatible
        serialized_result = serializable_update_result(update_operation_result)
        
        return serialized_result.raw_result
    except RuntimeError as runtime_error:
        ### Handle Runtime Error and raise HTTP exception with status code 400
        raise HTTPException(status_code=400, detail=str(runtime_error))
### FETCH_TAG update_notes

### FETCH_TAG is_crystal_already_fished
@app.get("/is_crystal_already_fished/{plate_id}/{well_id}")
async def is_crystal_already_fished(plate_id: str, well_id: str):
    """
    API endpoint to check if the crystal from a given plate and well is already fished.

    Parameters:
        plate_id (str): ID of the plate.
        well_id (str): ID of the well.

    Returns:
        dict: Contains the result in the format {"result": bool}.

    Raises:
        HTTPException: If there's a RuntimeError, returns a 400 status code with the error detail.
    """
    try:
        ### Call the utility function to get the fished status
        result = client.is_crystal_already_fished(plate_id, well_id)
        
        ### Return the result as a dictionary
        return {"result": result}
    except RuntimeError as runtime_error:
        ### Raise HTTP 400 error if RuntimeError occurs
        raise HTTPException(status_code=400, detail=str(runtime_error))
### FETCH_TAG is_crystal_already_fished

### FETCH_TAG update_shifter_fishing_result
@app.patch("/update_shifter_fishing_result")
async def update_shifter_fishing_result(request: UpdateShifterFishingResultRequest) -> Any:
    """
    Update the shifter fishing result for a well.

    Parameters:
    - request: The incoming request containing well_shifter_data, xtal_name_index, and xtal_name_prefix.

    Returns:
    - A serializable update result object.

    Raises:
    - HTTPException: An exception is raised for any runtime errors.
    """

    try:
        ### Perform the update action by invoking the utility function
        result = client.update_shifter_fishing_result(
            request.well_shifter_data,
            request.xtal_name_index,
            request.xtal_name_prefix
        )

        ### Serialize the result for a standard output
        update_result = serializable_update_result(result)

        return update_result

    except RuntimeError as e:
        ### Handle runtime exceptions by returning HTTP 400 status code
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG update_shifter_fishing_result

### FETCH_TAG import_fishing_results
@app.post("/import_fishing_results")
async def import_fishing_results(fishing_results: List[dict]):
    """
    Import fishing results into the ffcs_db server.

    This endpoint accepts a list of dictionaries containing fishing results and
    delegates the processing to the import_fishing_results method in the client.
    
    Returns:
        - A serialized result of the last `update_shifter_fishing_result` operation.
    
    Raises:
        - HTTPException: with status_code 400 if any RuntimeError is caught.
    """
    try:
        ### Delegate the operation to client's import_fishing_results function
        result = client.import_fishing_results(fishing_results)
        
        ### Serialize the update result
        update_result = serializable_update_result(result)
        
        return update_result
    except RuntimeError as e:
        ### Raise an HTTPException for any caught RuntimeError
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG import_fishing_results

### FETCH_TAG find_user_from_plate_id
@app.get("/find_user_from_plate_id/{plate_id}")
async def find_user_from_plate_id(plate_id: str) -> Any:
    """
    Find a user and campaign ID based on the given plate ID.

    This FastAPI endpoint receives a plate ID, then finds the corresponding
    user and campaign ID from the database. If the plate ID does not exist,
    it returns None for both the user and the campaign ID.

    Parameters:
    plate_id (str): The plate ID to look for.

    Returns:
    dict: A dictionary containing the user and campaign ID if found, otherwise None.

    Exceptions:
    HTTPException: Raised if there is a RuntimeError during the execution.
    """
    try:
        result = client.find_user_from_plate_id(plate_id)
        if result:
            return {"user": result["user"], "campaign_id": result["campaign_id"]}
        else:
            ### No matching plate found in the database.
            return {"user": None, "campaign_id": None}
    except RuntimeError as e:
        ### Log the RuntimeError for debugging purposes.
        print(f"RuntimeError occurred: {e}")
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG find_user_from_plate_id

### FETCH_TAG find_last_fished_xtal
@app.get("/find_last_fished_xtal/{user}/{campaign_id}")
async def find_last_fished_xtal(user: str, campaign_id: str):
    """
    Fetch the last fished xtal based on user and campaign ID.
    
    Parameters:
    - user: The user account identifier
    - campaign_id: The identifier for the campaign

    Returns:
    A JSON object containing the fished xtal details.

    Raises:
    HTTPException if unable to fetch the fished xtal
    """
    try:
        ### Fetch fished xtals from database using the client utility function
        fetched_xtals = client.find_last_fished_xtal(user, campaign_id)
        
        ### Convert ObjectIds to strings for JSON serialization
        for xtal in fetched_xtals:
            xtal["_id"] = str(xtal["_id"])
        
        return {"result": fetched_xtals}
    except RuntimeError as runtime_error:
        ### Handle errors by raising an HTTP Exception
        raise HTTPException(status_code=400, detail=str(runtime_error))
### FETCH_TAG find_last_fished_xtal

### FETCH_TAG get_next_xtal_number
@app.get("/get_next_xtal_number/{plate_id}")
async def get_next_xtal_number(plate_id: str):
    """
    Retrieve the next available crystal number based on the given plate ID.
    
    :param plate_id: The plate identifier as a string
    :return: A dictionary containing the next available crystal number
    :raises HTTPException: If the user for the given plate cannot be found
    """
    try:
        ### Fetch the next available xtal number using client utility function
        next_number = client.get_next_xtal_number(plate_id)
        
        ### Return the result as a JSON response
        return {"next_xtal_number": next_number}
    except RuntimeError as e:
        ### Handle runtime errors by returning an HTTP 400 status code
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_next_xtal_number

### FETCH_TAG get_soaked_wells
@app.get("/get_soaked_wells/{user}/{campaign_id}")
async def get_soaked_wells(user: str, campaign_id: str):
    """
    Endpoint to retrieve wells that are soaked but not yet fished for a given user and campaign.

    :param user: The user account
    :param campaign_id: The campaign identifier
    :return: JSON response containing the list of soaked but not fished wells
    """
    try:
        # Fetch soaked wells using the utility function
        result = client.get_soaked_wells(user, campaign_id)

        # Convert MongoDB ObjectIds to string format for JSON serialization
        if result:
            for well in result:
                well["_id"] = str(well["_id"])
                well["libraryId"] = str(well["libraryId"])

        # Return the result as a JSON response
        return {"result": result}

    except RuntimeError as e:
        # Handle runtime errors and send HTTP 400 status
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_soaked_wells

### FETCH_TAG get_number_of_unsoaked_wells
@app.get("/get_number_of_unsoaked_wells/{user}/{campaign_id}")
async def get_number_of_unsoaked_wells(user: str, campaign_id: str):
    """
    Fetch the number of unsoaked wells for a specific user and campaign ID.
    :param user: The username to query.
    :param campaign_id: The campaign ID to query.
    :return: A dictionary containing the number of unsoaked wells.
    """
    try:
        ### Fetch the count of unsoaked wells using the utility function from the client
        unsoaked_count = client.get_number_of_unsoaked_wells(user, campaign_id)
        
        ### Return the number of unsoaked wells in a dictionary format
        return {"number_of_unsoaked_wells": unsoaked_count}
    except RuntimeError as e:
        ### Catch any runtime errors and return an HTTP 400 error
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_number_of_unsoaked_wells

### FETCH_TAG update_soaking_duration
@app.put("/update_soaking_duration")
async def update_soaking_duration(data: UpdateSoakDurationData):
    """
    Update the soakDuration field for wells based on the input data.
    
    Parameters:
        data (UpdateSoakDurationData): The input data containing user, campaign_id, and wells information.
    
    Returns:
        dict: A dictionary containing the update result, or an error message.
    """
    # Convert string representations to appropriate data types for well fields
    for well in data.wells:
        try:
            well['_id'] = bson.objectid.ObjectId(well['_id'])
            well['soakTransferTime'] = datetime.fromisoformat(well['soakTransferTime'])
        except Exception as conversion_error:
            print(f"Data conversion failed: {conversion_error}")
            return {"error": "Data conversion failed"}

    # Update the soak duration using utility function
    try:
        update_result = client.update_soaking_duration(
            data.user,
            data.campaign_id,
            data.wells
        )
    except Exception as update_error:
        print(f"Failed to update soaking duration: {update_error}")
        return {"error": "Failed to update soaking duration"}

    return update_result
### FETCH_TAG update_soaking_duration

### FETCH_TAG get_all_fished_wells
@app.get("/get_all_fished_wells/{user}/{campaign_id}")
async def get_all_fished_wells(user: str, campaign_id: str):
    """
    FastAPI endpoint to fetch all fished wells for a given user and campaign ID.
    
    Parameters:
    - user (str): The user account identifier.
    - campaign_id (str): The campaign identifier.
    
    Returns:
    - JSON object containing the list of fished wells.
    
    Raises:
    - HTTPException: An exception with a 400 status code if an error occurs.
    """
    try:
        ### Fetch all fished wells from the utility function
        wells = client.get_all_fished_wells(user, campaign_id)
        
        ### Convert MongoDB ObjectIds to strings for JSON serialization
        for well in wells:
            well["_id"] = str(well["_id"])
        
        return {"fished_wells": wells}
        
    except Exception as e:
        ### Handle exceptions by raising HTTP errors with detailed messages
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_all_fished_wells

### FETCH_TAG get_all_wells_not_exported_to_datacollection_xls
@app.get("/get_all_wells_not_exported_to_datacollection_xls/{user}/{campaign_id}")
async def get_all_wells_not_exported_to_datacollection_xls(user: str, campaign_id: str):
    """
    Endpoint for fetching all well data not exported to datacollection xls.
    Args:
        user (str): The user account.
        campaign_id (str): The campaign ID.
    Returns:
        dict: A dictionary containing a list of well data that meet the conditions.
    """
    try:
        wells = client.get_all_wells_not_exported_to_datacollection_xls(user, campaign_id)
        for well in wells:
            well["_id"] = str(well["_id"])
        return {"wells_not_exported_to_xls": wells}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_all_wells_not_exported_to_datacollection_xls

### FETCH_TAG mark_exported_to_xls
@app.put("/mark_exported_to_xls")
async def mark_exported_to_xls(data: MarkExportedToXlsData):
    """
    Endpoint for marking wells as exported to XLS.
    
    :param data: Payload containing well data.
    :return: A dictionary containing the update result.
    """
    # Convert _id to ObjectId type
    for well_data in data.wells:
        well_data['_id'] = bson.objectid.ObjectId(well_data['_id'])
    
    # Use utility function to mark wells as exported
    update_result = client.mark_exported_to_xls(data.wells)
    return update_result
### FETCH_TAG mark_exported_to_xls

### FETCH_TAG send_notification
@app.post("/send_notification/{user_account}/{campaign_id}/{notification_type}")
async def send_notification(user_account: str, campaign_id: str, notification_type: str):
    """
    API endpoint to send a notification.

    Parameters:
    - user_account (str): The account to which the notification is sent.
    - campaign_id (str): The ID of the campaign related to the notification.
    - notification_type (str): The type of the notification.

    Returns:
    dict: A dictionary containing the status and inserted_id if successful, or raises an HTTPException.
    """
    try:
        result = client.send_notification(user_account, campaign_id, notification_type)
        if result:
            return {"status": "success", "inserted_id": str(result.inserted_id)}
        else:
            raise HTTPException(status_code=500, detail="Notification not sent.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG send_notification

### FETCH_TAG get_notifications
@app.get("/get_notifications/{user_account}/{campaign_id}/{timestamp}")
async def get_notifications(user_account: str, campaign_id: str, timestamp: datetime):
    """
    Endpoint to retrieve notifications for a specified user account, campaign, and timestamp.

    Args:
        user_account (str): The user account to filter notifications for.
        campaign_id (str): The campaign ID to filter notifications for.
        timestamp (datetime): The starting timestamp for filtering notifications.

    Returns:
        dict: A dictionary containing the list of notifications under the key "notifications".

    Raises:
        HTTPException: If any error occurs during the operation.
    """
    try:
        ### Fetch notifications using utility function
        notifications = client.get_notifications(user_account, campaign_id, timestamp)
        return {"notifications": notifications}
    except Exception as e:
        ### Handle exceptions by raising an HTTPException
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_notifications

### FETCH_TAG add_fragment_to_well
@app.post("/add_fragment_to_well/")
async def add_fragment_to_well(fragment_request: FragmentRequest):
    """
    FastAPI endpoint to add a fragment to a specified well. It accepts a request with the details of the library,
    well, fragment, and other related parameters, and then calls a function to perform the database update.

    Args:
        fragment_request (FragmentRequest): An object representing the request with necessary details to add a fragment to a well. 
                                            It includes information about the library, well ID, fragment details, solvent volume, 
                                            ligand transfer volume, ligand concentration, and whether it's a solvent test.

    Returns:
        JSONResponse: A JSON response containing the result of the add fragment operation, including details such as the number 
                      of modified documents and the status of the operation.

    Raises:
        HTTPException: Raises an HTTPException with status code 400 in case of any exceptions during the process.
    """
    try:
        # Convert string ID representations to ObjectId
        fragment_request.library['_id'] = ObjectId(fragment_request.library['_id'])
        fragment_request.well_id = ObjectId(fragment_request.well_id)

        # Perform the add fragment operation
        response = client.add_fragment_to_well(
            fragment_request.library,
            fragment_request.well_id,
            fragment_request.fragment,
            fragment_request.solvent_volume,
            fragment_request.ligand_transfer_volume,
            fragment_request.ligand_concentration,
            fragment_request.is_solvent_test
        )
        return {"result": response}
    except Exception as e:
        # Handle exceptions and return an appropriate HTTP response
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG add_fragment_to_well

### FETCH_TAG remove_fragment_from_well
@app.post("/remove_fragment_from_well/")
async def remove_fragment_from_well(well_id: str):
    """
    FastAPI endpoint to remove a fragment from a specified well in the database. This endpoint
    receives a well ID, converts it to a MongoDB ObjectId, and uses a client function to
    perform the removal operation in the database.

    Args:
        well_id (str): The string representation of the MongoDB ObjectId of the well from which
                       the fragment is to be removed.

    Returns:
        dict: A dictionary containing the result of the removal operation, which includes 
              'nModified' for the count of modified documents, 'ok' to indicate success, and 
              'n' for the count of matched documents.

    Raises:
        HTTPException: If any exception occurs during the database operations, encapsulates 
                       the error in an HTTP response with status code 400.
    """
    try:
        response = client.remove_fragment_from_well(ObjectId(well_id))
        return {"result": response}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG remove_fragment_from_well

### FETCH_TAG import_library
@app.post("/import_library/")
async def import_library(library: dict) -> dict:
    """
    FastAPI endpoint to import a new library into the database.

    This endpoint accepts a dictionary representing a library, converts the 'libraryBarcode' to a MongoDB ObjectId,
    and then calls the import_library method from the client to insert the library into the database.
    If the library is successfully inserted, the result is returned in a modified format with the '_id' converted to a string.
    If a LibraryAlreadyImported exception occurs, an HTTPException with status code 400 is raised.

    Args:
        library (dict): A dictionary representing the library to be imported.

    Returns:
        dict: A dictionary with the result of the import operation, including the '_id' of the imported library.

    Raises:
        HTTPException: If the library has already been imported or if any other exception occurs.
    """
    library['libraryBarcode'] = ObjectId(library['libraryBarcode'])
    try:
        result = client.import_library(library)
        result['_id'] = str(result['_id'])
        return {"result": result}
    except LibraryAlreadyImported as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG import_library

### FETCH_TAG get_campaign_libraries
@app.post("/get_campaign_libraries/")  # We use POST because we are sending user & campaign_id in the request body
async def get_campaign_libraries(query: CampaignRequest):
    """
    FastAPI endpoint to retrieve all libraries associated with a given user and campaign ID.
    
    This endpoint accepts a POST request containing a user and campaign ID, and returns a list of 
    libraries associated with these identifiers. Each library's ObjectId is converted to a string 
    for better compatibility and ease of use.

    Args:
        query (CampaignRequest): A model representing the user and campaign ID for the query.

    Returns:
        list: A list of dictionaries, each representing a library. The '_id' and 'libraryBarcode'
              fields are converted to strings.

    Raises:
        HTTPException: If an exception occurs during the retrieval process.
    """
    try:
        libraries = client.get_campaign_libraries(query.user, query.campaign_id)
        for library in libraries:
            library['_id'] = str(library['_id'])  # Convert the ObjectId to string for each library
            if 'libraryBarcode' in library:
                library['libraryBarcode'] = str(library['libraryBarcode'])  # Convert the ObjectId to string
        return libraries
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
### FETCH_TAG get_campaign_libraries


### FETCH_TAG get_library_usage_count
@app.get("/get_library_usage_count/")
async def get_library_usage_count(user: str, campaign_id: str, library_id: str):
    """
    FastAPI endpoint for retrieving the usage count of a specific library within a given user account 
    and campaign. It invokes a function that queries the database to count the number of wells associated 
    with the specified library.

    Args:
        user (str): The identifier of the user account.
        campaign_id (str): The identifier of the campaign.
        library_id (str): The identifier of the library.

    Returns:
        dict: A dictionary with a single key 'count', holding the integer value of the usage count.

    Raises:
        HTTPException: If any exception occurs during the process, it raises an HTTPException with 
                       status code 400 and a detailed error message.
    """
    try:
        count = client.get_library_usage_count(user, campaign_id, library_id)
        return {"count": count}
    except Exception as e:
        # Providing a detailed error message for easier troubleshooting
        raise HTTPException(status_code=400, detail=f"Failed to retrieve library usage count: {e}")
### FETCH_TAG get_library_usage_count

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
