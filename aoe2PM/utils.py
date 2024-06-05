from mgz.model import parse_match 

def extract_recordfile_data(filepath):
    ''' Extracts match data from a given .aoe2record file and returns a dictionary with the following keys:
    'match_info': dictionary with general match information
    'init_objects': dictionary with initial objects for each player
    'inputs': dictionary with all inputs for each player
    '''


    #prepare dicts
    match_info = {}
    init_objects = {}
    inputs = {}

    #parse match
    with open(filepath, 'rb') as data:
        match = parse_match(data)
        
    #get general game info  
    match_info['match_id'] =  filepath.split('_')[-1].split('.')[0]
    match_info['map'] = match.map.name
    match_info['map_size'] = match.map.size
    match_info['duration'] = match.duration.seconds
    match_info['start_time'] = match.timestamp   

    #gather general player information
    match_info['players']   = {}
    excludes = ['objects']

    #define_teams 
    match_info['teams'] = {1:[],2:[]}
    player_mapping = {}

    for player in match.players:
        #get player info
        p_idx = player.number
        p_id = player.profile_id
        p_name = str(player.name)

        #create mapping to read id from name
        player_mapping[p_name] = p_id


        #convert from name to id and create team dict
        for t_idx, team in enumerate(match.teams):
            if p_name in [str(pn) for pn in team]:
                match_info['teams'][t_idx+1].append(p_id)
            

        match_info['players'][p_idx] = {}  
        for key in player.__dict__.keys():
            if key not in excludes:
                match_info['players'][p_idx][key] = player.__dict__[key]

    #determine winner
    if match.players[0].winner == True:
        match_info['winner'] = int(1)
    else:
        match_info['winner'] = int(2)

    ##read initial objects for each player
    for player in match.players:
        init_objects[player.profile_id] = {}

        for object in player.objects:
            
            #catch initial objects without name
            if object.__dict__['name'] == None:
                continue
            
            #catch multiple TC instances and Flare Objects
            if object.__dict__['object_id'] in [618,619,620,332]:
                continue


            #create dict for each instance
            instance_id = object.__dict__['instance_id']
            init_objects[player.profile_id][instance_id] = {}

            init_objects[player.profile_id][instance_id]['object_type'] = object.__dict__['object_id']
            init_objects[player.profile_id][instance_id]['name'] = object.__dict__['name']
            init_objects[player.profile_id][instance_id]['object_id'] = object.__dict__['instance_id']
            init_objects[player.profile_id][instance_id]['object_class'] = object.__dict__['class_id']
            init_objects[player.profile_id][instance_id]['position'] = object.__dict__['position']



    #get all item names
    for object in match.inputs:
        if not str(getattr(object,'type')) == "Chat":
            attr_list = object.__dict__.keys()
            break

                
    ##read inputs
    exclude_attrs = ['player','payload']
    exclude_types = ['Chat','Flare','Move','Order']


    for player in match.players:
        p_id = player_mapping[str(player.name)]
        
        inputs[p_id] = {}

        idx = 0

        for object in match.inputs:

            if player_mapping[str(object.player)] == p_id:
                inputs[p_id][idx] = {}

                #skip for exclude types
                if str(getattr(object,'type')) in exclude_types:
                    continue
                
                #add player_id to inputs
                inputs[p_id][idx]['player_id'] = p_id
                inputs[p_id][idx]['p_idx'] = object.player.number

                for attribute in attr_list:
                    if attribute not in exclude_attrs:
                        inputs[p_id][idx][attribute] = str(getattr(object,attribute))

                #read payload as dict
                inputs[p_id][idx]['payload'] = getattr(object,'payload')

                #read timestamp as datetime.timedelta object
                inputs[p_id][idx]['timestamp'] = getattr(object,'timestamp')
                inputs[p_id][idx]['microseconds'] = inputs[p_id][idx]['timestamp'].microseconds
                
                #modify payload for gather_object_ids
                if str(getattr(object,'type')) in ['Gather', 'Target']:
                    mod_oids = []
                    for o_id in inputs[p_id][idx]['payload']['object_ids']:
                        mod_oids.append(int(o_id/(2**16)))
                    
                    inputs[p_id][idx]['payload']['object_ids'] = mod_oids

                
                idx +=1

    #remove duplicate tech research
    for p_id in inputs:
        research_dict = {}

        for key,input in inputs[p_id].items():
            if input.get("type",None)=="Research":
                tech = input["param"]

                if tech in research_dict:
                    research_dict[tech].append(key)
                else: research_dict[tech] = [key]


        #read all keys but the last from research_dict into to_del where len()>1 
        for tech in research_dict:
            if len(research_dict[tech]) > 1:
                for key in research_dict[tech][:-1]:
                    del inputs[p_id][key]


    #read gaia data
    gaia_dict = {}

    for object in match.gaia:
        gaia_dict[object.instance_id] = {'type':object.name,
                                'position':object.position}

    #return dict with match data
    return {'match_info': match_info, 
            'init_objects':init_objects,
            'inputs': inputs,
            'gaia': gaia_dict}

            




   

            