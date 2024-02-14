# Helper functions

def dict_to_tuple(alist):
    """
    Tested: Works 
    Converts a list of dictonaries to a list of tuples, with a seperate list of the dict keys
    Requires python 3.6+ since standard dicts are ordered
    Input: [ {A:B,...},... ]
    Output:[ (B,...), ... ], [A,..]
    """
    keys = []
    first_loop = True
    lst = []
    
    for item in alist:
        _ = [] #this will be converted to a tuple
        
        for key,value in item.items():
            if first_loop == True:
                keys.append(key)
            _.append(value)
        
        first_loop = False
        tple = tuple(_)
        lst.append(tple)
    
    return lst,keys

def read_config(config_file):
    '''
    Tested: 
    
    Input: configfile relative path (?) i think
    Output: dictionary of configs
    '''
    config = configparser.ConfigParser()
    config.read(config_file)
    
    settings = {
        "symbols": config['symbols']['symbols'].split(', '),
        "exchanges": config['exchanges']['exchanges'].split(', '),
        "btc_inverse_perp": config['symbols']['btc_inverse_perp'],
        "btc_linear_perp": config['symbols']['btc_linear_perp'],
        "timeframe": config['settings']['timeframe'],
        "orderbook_depth": int(config['settings']['orderbook_depth']),
        "timeout": int(config['settings']['timeout']),
        "candle_limit": int(config['settings']['candle_limit']),
        "username": config['credentials']['user'],
        "password": config['credentials']['password'],
        "host": config['credentials']['host'],
        "port": config['credentials']['port'],
        "db_name": config['credentials']['dbname']
    }
    return settings

def get_config_value(config, section, option, default=None, value_type=str):
    '''
    Error handling for config file reading
    
    '''
    try:
        value = config.get(section, option)
        if value_type is not None:
            return value_type(value)
        return value
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
        return default
