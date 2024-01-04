import datetime


def PlateDataSchema(user_account: str, campaign_id: str, plate_id: str, drop_volume: float,
                    plate_type='SwissCl', imagining_start=None):

    plate_data_template = {
        'userAccount': str(user_account),
        'plateId':   str(plate_id),
        'campaignId': str(campaign_id),
        'plateType': str(plate_type),
        'dropVolume': float(drop_volume),
        'batchId': None,
        'createdOn': None,
        'lastImaged': None,
        'soakPlacesSelected': False,
        'soakStatus': None,
        'soakExportTime': None,
        'soakTransferTime': None,
        'cryoProtection': False,
        'redesolveApplied': False}

    # Check for all the required keys
    if plate_id == '' or plate_id is None:
        raise Exception('ffcsdbclient - Plate Data Schema - PlateId cannot be empty.')

    try:
        plate_id = int(plate_id)
    except ValueError:
        raise Exception('Plate Data Schema - Plate Id needs to contains only numbers!')

    if campaign_id == '' or campaign_id is None:
        raise Exception('ffcsdbclient - Plate Data Schema - CamapignId cannot be empty.')

    if user_account == '' or user_account is None:
        raise Exception('ffcsdbclient - Plate Data Schema -User Account cannot be empty.')

    if imagining_start is None:
        plate_data_template['createdOn'] = datetime.datetime.now()
    else:
        plate_data_template['createdOn'] = imagining_start

    if not isinstance(plate_data_template['createdOn'], datetime.datetime):
        raise Exception('ffcsdbclient - Plate Data Schema - imagining_start needs to be instance of datetime')

    return plate_data_template


def WellDataSchema(user_account: str, campaign_id: str, plate_id: str, well: str, well_echo: str,
                   x: int, y: int, x_echo: float, y_echo: float):

        well_template = {
            # 'crystalName': some_name,
            'userAccount': user_account,
            'campaignId': campaign_id,
            'plateId': str(plate_id),
            'well': well, # MRC3 notation, such as 'A12a'
            'wellEcho': well_echo,  # Echo does not use MRC3 notation. wellEcho is converted to 'well' converted to Echo notation, and is used only to export files to echo
            'x': int(x),  # in pixels
            'y': int(y),  # in pixels
            'xEcho': float(x_echo),  # pixels converted to [um] + offset for subwell 'd'
            'yEcho': float(y_echo),  # pixels converted to [um] + offset for subwell 'd'
            'libraryAssigned': False,
            'libraryName': None,
            'libraryBarcode': None,
            'libraryId': None,
            'solventTest': False, # if given well is used for solvent test
            'sourceWell': None,
            'smiles': None,
            'compoundCode': None,
            'libraryConcentration': None, # ligand concentration in Library
            'solventVolume': None,
            'ligandTransferVolume': None,
            'ligandConcentration': None, # ligand concentration in Drop
            'soakStatus': None,
            'soakExportTime': None,
            'soakTransferTime': None,
            'soakTransferStatus': None, # retrieved from Echo Transfer Reports .xml file
            'cryoProtection': False,
            'cryoDesiredConcentration': None,
            'cryoTransferVolume': None,
            'cryoSourceWell': None,
            'cryoStatus': None,
            'cryoExportTime': None,
            'cryoTransferTime': None,
            'cryoName': None,
            'cryoBarcode': None,
            'redesolveApplied': False,
            'redesolveName': None,
            'redesolveBarcode': None,
            'redesolveSourceWell': None,
            'redesolveTransferVolume': None,
            'redesolveStatus': None,
            'redesolveExportTime': None,
            'shifterComment': None,   # OK: Mounted, Failed: Melted, etc... messages from Shifter
            'shifterXtalId': None,    # Internal Shifter Identification of Xtals, ususally Xtal-1,...
            'shifterTimeOfArrival': None,
            'shifterTimeOfDeparture': None,
            'shifterDuration': None,
            'puckBarcode': None,      # shifter CSV field: destinationName
            'puckPosition': None,     # shifter CSV field: destinationLocation
            'pinBarcode': None,       # shifter CSV field: barcode
            'puckType': None,         # shifter CSV field: externalComment
            'fished': False,          # indicates if xtal was fished. Set in ffcsdbclient
            'xtalName': None,         # name for the xtal given to it if it is fished successfully. Set in ffcsdbclient
            'soakDuration': None,      # ! in seconds! how long the xtal has been soaking. Update by SoakTimer thread running by ffcsGui
            'notes': None,
            'exportedToXls': False
        }

        validate_str_keys = ['userAccount', 'campaignId', 'plateId', 'well', 'wellEcho']
        validate_int_keys = ['x', 'y']
        validate_float_keys = ['xEcho', 'yEcho']

        # Check for all the required keys
        for key in validate_str_keys:
            if well_template[key] == '' or well_template[key] is None:
                raise Exception('ffcsdbclient - Well Data Schema - {} cannot be empty.'.format(key))

        for key in validate_int_keys:
            if not isinstance(well_template[key], int):
                raise Exception('ffcsdbclient - Well Data Schema - {}  must be int.'.format(key))

        for key in validate_float_keys:
            if not isinstance(well_template[key], float) and not isinstance(well_template[key], int):
                raise Exception('ffcsdbclient - Well Data Schema - {}  must be float or int.'.format(key))

        return well_template