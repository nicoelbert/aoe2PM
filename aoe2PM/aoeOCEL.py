from aoe2PM import utils
from datetime import datetime
from itertools import combinations
import pandas as pd
from datetime import timedelta
import json
import math
from itertools import combinations
import os
import sqlite3
import datetime
import random
import bisect


class Event:
    def __init__(self, p_idx, g_idx, input):
        self.p_idx = p_idx
        self.g_idx = g_idx
        self.activity = input['type'] + ' ' + input['param']
        #convert input timestamp to timestamp field
        self.timestamp = input['timestamp']
        self.microseconds = input['timestamp'].microseconds
        self.object_ids = input['payload'].get('object_ids', [])
        self.activity_type = input['type']
        self.activity_subtype = input['param']
        self.activity = input['type'] + ' ' + input['param']
        self.target_type = input['payload'].get('target_type', None)
        self.target_id =  input['payload'].get('target_id', None)
        self.player_id = input['player_id']
        self.payload = input['payload']

        self.position = input.get('position', None)

        #read from masterdata later
        self.duration = 0 #duration can be read from master data, can be adjusted for buildings dependent on building time and number of villagers
        self.cost = 0 # can be read from masterdata dict

        #execution times
        self.projected_start = None
        self.projected_termination = None
        self.actual_start = None
        self.actual_termination = None

        #execution object info
        self.object_info = None


    def __str__(self):
        return f"{self.activity_type} {self.activity_subtype} at {self.timestamp} by {self.object_ids}"
    

    def get_dict(self):
        return {
            'p_idx': self.p_idx,
            'g_idx': self.g_idx,
            
            
                 'activity': self.activity,
            'timestamp': self.timestamp,
            'object_ids': self.object_ids,
            'activity_type': self.activity_type,
            'activity_subtype': self.activity_subtype,
            'target_type': self.target_type,
            'target_id': self.target_id,
            'player_id': self.player_id,
            'payload': self.payload,
            'duration': self.duration,
            'cost': self.cost,
            'projected_start': self.projected_start,
            'projected_termination': self.projected_termination,
            'actual_start': self.actual_start,
            'actual_termination': self.actual_termination,
            'position': self.position
        }



class Object:
    def __init__(self, id, category = "Inititial", type = "Initial", player = "Uknown"):

        #self.id =
        #self.name = f'{type}_{idx}' # some speaking ID read from a dict + a numeric indicator e.g. "TC01", "VIL02", etc or "BTC01" for build town center and "UVIL02" for Villager
        self.id = id #object ID from dataset, since not all object IDs are known 
        self.type = type #object type, e.g. "Villager", "Town Center", "Military", etc.
        self.category = category #can be read from a masterdata dict, e.g. building, unit, etc.
        self.player = player
        
        self.queue_events = []
        self.other_events = []
        self.executed_events = []


        #attributes
        self.events = []    #list of events, for units might be queueing event, creattion event, etc
                            # for buildings might be construction, completion event + the production events that happen in the building
                            # hence the queue would be replayed to create all the events that happen in the building in combination with durations

        self.relations = {} # dict from object relations

    def __str__(self):
        return f"{self.category} - {self.type} -  ID {self.id} belonging to {self.player}"
    
    def run_queue(self):
        for event in self.queue_events:
            self.events.append(event)
    
    def get_info_str(self):
        return f"{self.category} - {self.type} -  ID {self.id} belonging to {self.player}"


    def add_event(self, event):
        """Add event to object's event list. If the event is a queue event, add it to the queue_events list."""
        if event.activity_type == 'Queue' or event.activity_type == 'Research' or event.activity_type == 'Unqueue':
            self.queue_events.append(event)
        else:
            self.other_events.append(event)

class OCEL_event:
    def __init__(self,event_type,time,player,attributes_dict:dict = None, init_act_id = None, queue_info = None):
    
        
        self.event_id    = None
        
        
        self.ocel_time = time
        self.ocel_type = event_type
        self.player = player

        #for mapping of initial objects
        self.init_act_id = init_act_id

        #for mapping of queue objects
        self.queue_info = queue_info

        #attributes
        self.attributes = {
            'Player': [],
            'Villager': [],
            'Building': [],
            'Match': [],
            'Session': [],
            'Unit': []
        }

        if attributes_dict:
            self.attributes.update(attributes_dict)

    def __str__(self):
        return f"{self.event_id}: {self.ocel_type} at {self.ocel_time} by {self.player} - Attributes: {self.attributes}"


    def get_table_dict(self):
        return {
            'ocel_id': self.event_id,
            'ocel_time': self.ocel_time,
            'ocel_type': self.ocel_type,
            'object_types': self.attributes
        }
class OCEL_object:
    def __init__(self,id:str,type:str,player:str,creation_time=None,act_id = None, attributes= {}):


        self.ocel_id = id
        self.ocel_type = type

        self.creation_time = None

        self.player = player
        self.act_id = act_id

        self.events = []

        self.attributes = attributes


    def __str__(self):
        return f"{self.ocel_type} {self.ocel_id} belonging to {self.player}"
    



def exportOCEL_fromRecordfile(match_ids:list, recordfile_path = './data/recordfiles/', goal = 10000000000, db_path = './out/aoe_data_ocel.sqlite', masterdata_path = '.data/masterdata'):
    """ Export OCEL Data from Recordfiles using MatchIds and recordfile path. Number of matches to be processed succesfully can be limited by goal parameter.
    """

    ####################################### Master Data ########################################
    
    ##Open Masterdata
    with open(masterdata_path + '/building_actions.json') as f:
        building_actions = json.load(f)

    with open(masterdata_path + '/base_build_times.json') as f:
        base_build_times = json.load(f)

    with open(masterdata_path + '/build_gather_map.json') as f:
        gather_actions = json.load(f)#

    with open(masterdata_path + '/gather_map.json') as f:
        gather_map = json.load(f)#

    event_duration_seconds = {}

    #parse actions
    for build, dict in building_actions.items():
        for key in dict:
            event_duration_seconds[key] = dict[key]
            
    build_action_mapping = {}

    for build, dict in building_actions.items():
        for key in dict:
            build_action_mapping[key] = build


        
    build_times = {}

    for bt in base_build_times:
        build_times["Build " + bt] = base_build_times[bt]


    ####################################### Cleaning Functions ########################################

    def rename_object(object_name, match_id):
        #check if object_name is numeric
        if str(object_name).isnumeric():
            object_name = f"M{match_id}_UO{object_name}"
            return object_name

        object_name_split = object_name.split(" ")
        object_name_split.insert(-1, f"M{match_id}")
        object_name = "_".join(object_name_split)
        return object_name

    def event_format_timestamps(dt):
        # Format the datetime object as a string in the specified format
        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_time

    def object_format_timestamps(timestamp):
        if timestamp == None:
            timestamp = 0
        # Check if the timestamp is likely in milliseconds (common in some systems)
        if timestamp > 1e10:  # This is an arbitrary cutoff for millisecond timestamps
            timestamp /= 1000  # Convert milliseconds to seconds
        
        # Convert the timestamp to a datetime object
        dt = datetime.datetime.fromtimestamp(timestamp)
        # Format the datetime object as a string in the specified format
        formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_time

    def clean_str(str):
        remove_list = ['.','-',':','(',')'," ","/"]
        for char in remove_list:
            str = str.replace(char, "")

        return str

    def clean_special(str):
        remove_list = ['(',')','.']
        for char in remove_list:
            str = str.replace(char, "")

        return str
    
    ####################################### Execution Functions ########################################
    def execute_builds(global_events):

        OCEL_event_match = []
        OCEL_event_creation = []

        build_type_counter = {}


        for event_object in global_events:
            event = event_object.get_dict()

            p_id = event["player_id"]

            #add villagers
            if "Build" in event['activity']:
                #acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Unit", "Type": "Villager"}
                vills = event['payload']['object_ids']
                vill_count = len(vills)

                type = event["activity"]

                #update build type counter
                build_type = event['activity_subtype']
                if build_type in build_type_counter:
                    build_type_counter[build_type] += 1
                else:  
                    build_type_counter[build_type] = 1
                build_id = build_type + " " + str(build_type_counter[build_type])


                coordiante = event["position"]

                event_time = event["timestamp"]

                base_time = build_times.get(event["activity"], 0)
                build_time = base_time/math.sqrt(vill_count)
                
                #print(type,build_id,vill_count,vills,coordiante,build_time,event_time)

                #create OCEL event for each build
                event_type = "Start " + event["activity"]
                event_time = event["timestamp"]
                attributes_dict = {
                    'Player': [event["player_id"]],
                    'Villager': [vill_id for vill_id in vills],

                    #building id needs to be replaced with running value
                    'Building': [build_id]
                }
                

                #start event
                start_event = OCEL_event(event_type, event_time,p_id, attributes_dict=attributes_dict)
            

                #create End Event 
                event_type = "Complete " + event["activity"]
                event_time = event["timestamp"] + timedelta(seconds=build_time)                

                #remove player attribute for end event
                attributes_dict['Player'] = []

                end_event = OCEL_event(event_type, event_time,p_id, attributes_dict=attributes_dict)
                OCEL_event_match += [start_event, end_event]


                #create subsequent gather event after build
                if event["activity"] in gather_actions:
                    event_type = random.choice(gather_actions[event["activity"]])
                    event_type = event_type
                    gather_event = OCEL_event(event_type, event_time,p_id, attributes_dict=attributes_dict)
                    OCEL_event_match.append(gather_event)



                OCEL_event_creation.append(start_event   )

        return OCEL_event_match, OCEL_event_creation
    

    def execute_queue(object, p_id):
        """ Realizes the Queue Exectution of a Queue Object"""
        
        
        OCEL_event_match = []
        OCEL_event_creation = []

        #for debug purposes
        queue_execution = []

        # add durations
        for event in object.queue_events:
            event.duration = event_duration_seconds.get(event.activity, 0)

        busy_until = timedelta(seconds=0)
        projected_events = []

        # append time when event is succesfully executed
        for e_idx, event in enumerate(object.queue_events):
            # update queue and check for terminated events #create list to remove, bc we loop over queue
            exec_events = []

            for proj_event in projected_events:
                if proj_event.projected_termination <= event.timestamp:
                    exec_events.append(proj_event)
                    
            # execute events
            for exec_event in exec_events:
                exec_event.actual_start = exec_event.projected_start
                exec_event.actual_termination = exec_event.projected_termination
                projected_events.remove(exec_event)
                object.executed_events.append(exec_event)
                

                #create queue info to map events correctly
                queue_info = f'{p_id}_{object.id}_{exec_event.g_idx}'

                ######### FINISH EVENT # VID needs to be defined.
                attribute_dict = {
                    'Player': [p_id],
                    'Villager': [],
                    'Building': [object.id]
                }
                command_event = OCEL_event("Command " + exec_event.activity, exec_event.timestamp,p_id, attributes_dict=attribute_dict, queue_info=queue_info)

                #only command_event is connected to player
                attribute_dict = {
                    'Player': [],
                    'Villager': [],
                    'Building': [object.id]
                }
                start_event = OCEL_event("Start " + exec_event.activity, exec_event.actual_start, p_id, attributes_dict=attribute_dict, queue_info=queue_info) 
                end_event = OCEL_event("Complete " + exec_event.activity, exec_event.actual_termination, p_id, attributes_dict=attribute_dict, queue_info=queue_info)

                #this allows to only add command_event if executed, ignore unqueue for now
                OCEL_event_match += [command_event, start_event, end_event]
                OCEL_event_creation.append(end_event)
                
                queue_execution.append(f"Execute {exec_event.p_idx} {exec_event.activity} at {exec_event.actual_termination.seconds} - Queue: {[ (e.activity, e.p_idx, e.projected_termination.seconds) for e in projected_events ]}")
            
            # handle unqueues
            if event.activity_type == "Unqueue":
                slot_id = event.payload['slot_id']

                try:
                    # case current item is unqueued
                    if slot_id == 0 or len(projected_events) == 1:
                        # pop first item from queue since it is unqueued
                        unqueued_event = projected_events.pop(0)
                        unqueued_event.actual_termination = event.timestamp

                        # update unqueue event
                        event.projected_start = event.timestamp
                        event.projected_termination = event.timestamp
                        event.actual_start = event.timestamp
                        event.actual_termination = event.timestamp
                        object.executed_events.append(event)
                        
                        # update timestamp
                        busy_until = event.timestamp
                    
                    elif slot_id > 0:
                        unqueued_event = projected_events.pop(slot_id)

                        # update stats, no execution
                        unqueued_event.actual_start = None
                        unqueued_event.actual_termination = None
                        
                        # busy_until is projected_termination of event before the unqueued event
                        busy_until = projected_events[slot_id-1].projected_termination

                    # anyway update event info
                    event.activity_subtype = unqueued_event.activity_subtype
                except IndexError:
                    queue_execution.append(f"Unqueue at empty Queue at action {event.p_idx} {event.activity} at {event.timestamp.seconds}")

                # now update rest of the queue
                for proj_idx, proj_event in enumerate(projected_events[slot_id:]):
                    # update start time
                    proj_event.projected_start = busy_until
                    
                    # start first event directly
                    if proj_idx == 0:
                        proj_event.actual_start = busy_until

                    # update end time
                    busy_until += timedelta(seconds=proj_event.duration)
                    proj_event.projected_termination = busy_until

                    queue_execution.append(f"Unqueue {unqueued_event.p_idx} {unqueued_event.activity} at {event.timestamp.seconds}")

            else:  # handle normal queues
                # check if event can be executed directly
                if event.timestamp < busy_until:
                    event.projected_start = busy_until
                    busy_until += timedelta(seconds=event.duration)
                    event.projected_termination = busy_until
                    queue_execution.append(f"Queue Append {event.p_idx} {event.activity} at {event.timestamp.seconds} - Queue: {[ (e.activity, e.p_idx, e.projected_termination.seconds) for e in projected_events ]}")
                # otherwise append to queue
                else:
                    event.projected_start = event.timestamp
                    event.actual_start = event.timestamp
                    busy_until = event.timestamp + timedelta(seconds=event.duration)
                    event.projected_termination = busy_until
                    
                    ####### START EVENT 
                    queue_execution.append(f"Queue Instant {event.p_idx} {event.activity} at {event.timestamp.seconds} - Queue: {[ (e.activity, e.p_idx, e.projected_termination.seconds) for e in projected_events ]}")
                    
                projected_events.append(event)

        # execute remaining events
        for proj_event in projected_events:
            proj_event.actual_start = proj_event.projected_start
            proj_event.actual_termination = proj_event.projected_termination
            projected_events.remove(proj_event)
            object.executed_events.append(proj_event)
            queue_execution.append(f"Final Execute {proj_event.p_idx} {proj_event.activity} at {proj_event.actual_termination.seconds}")

        return OCEL_event_match, OCEL_event_creation, queue_execution
    

    ####################################### Main Function ########################################

    def oc_log_from_record(match_id,filepath):

        ## Extract Match Data per Player(Trace)
        match_data = utils.extract_recordfile_data(filepath)

        activities = []
        players = match_data['inputs'].keys()

        p_events = {}

        for p_id in players:
            p_inputs = match_data['inputs'][p_id]

            events = []
            e_idx = 0


            for key, input in p_inputs.items():

                #get amount for multiple hotkey actions with amount > 1
                try:
                    repeats = input['payload'].get('amount', 1)
                    
                    for it in range(repeats):
                        events.append(Event(f'e{e_idx}',"g_init",input))
                        
                        #increase event counter
                        e_idx += 1
                except: pass #print(input)


            p_events[p_id] = events


        ## Extract global Events
        global_events = []

        next_p_event = {}
        next_p_id = None

        g_idx = 0 

        for p_id in players:
            next_p_event[p_id] = p_events[p_id].pop(0)

        while True:

            #set earliest time to datetime.timedelta of 100 hours
            earliest_time = timedelta(hours=100)
            next_p_id = None
            
            for p_id in players:
                if next_p_event[p_id] is not None:
                    if next_p_event[p_id].timestamp < earliest_time:
                        try:
                            earliest_time = next_p_event[p_id].timestamp
                            next_p_id = p_id
                        except:
                            pass

            if earliest_time != timedelta(hours=100):
                #append to global_events
                next_p_event[next_p_id].g_idx = g_idx
                global_events.append(next_p_event[next_p_id])

                try:
                    next_p_event[next_p_id] = p_events[next_p_id].pop(0)
                except:
                    next_p_event[next_p_id] = None

                g_idx += 1
            else:
                break
                    

        ## Identify Acting Objects
        # Extract all acting Objects
        acting_objects_dict = {}
        assigned_objects = []


        for event_object in global_events:
            event = event_object.get_dict()


            #add buildings
            if "Queue" in event['activity']:
                for obj_id in event['object_ids']:
                    if obj_id in assigned_objects:
                        continue
                    assigned_objects.append(obj_id)
                    acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Building", "Type": build_action_mapping[event['activity']], "Time": event['timestamp']}
                    
            #special case markets
            if "Buy" in event['activity'] or "Sell" in event['activity']:
                for obj_id in event['object_ids']:
                    if obj_id in assigned_objects:
                        continue
                    assigned_objects.append(obj_id)
                    acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Building", "Type": "Market", "Time": event['timestamp']}

            #add villagers
            elif "Build" in event['activity'] or "Gather" in event['activity'] or "Reseed" in event['activity']:
                if "Gather Point" in event['activity']:
                    continue
                else:
                    for obj_id in event['object_ids']:
                        if obj_id in assigned_objects:
                            continue
                        assigned_objects.append(obj_id)
                        acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Unit", "Type": "Villager", "Time": event['timestamp']}

            #add military
            elif "Patrol" in event['activity'] or "Formation" in event['activity'] or "Stance" in event['activity']:
                for obj_id in event['object_ids']:
                        if obj_id in assigned_objects:
                            continue
                        assigned_objects.append(obj_id)
                        acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Unit", "Type": "Military Unit", "Time": event['timestamp']}

            elif "Research" in event['activity'] and "Age" not in event['activity']:
                for obj_id in event['object_ids']:
                    if obj_id in assigned_objects:
                        continue
                    assigned_objects.append(obj_id)

                    build_type = build_action_mapping.get(event['activity'], "Research Building")
                    if build_type == "Research Building":
                        #print(event['activity'], " could not be assigned to specific Building")
                        pass

                    acting_objects_dict[obj_id] = {"player": event_object.player_id, "id":obj_id, "Category": "Building", "Type": build_type, "Time": event['timestamp']}


        ## Remove gaia elemets from acting objects
        gaia_ids = match_data['gaia'].keys()
        aobj_ids = acting_objects_dict.keys()

        acting_gaia_ids = []

        for id in aobj_ids:
            if id in gaia_ids:
                acting_gaia_ids.append(id)

        for id in acting_gaia_ids:
            acting_objects_dict.pop(id)

        global_objects_df = pd.DataFrame(acting_objects_dict).T
        global_objects_df.sort_values('id', inplace=True)

        global_object_ids = global_objects_df.index.tolist()

        ## check for events with objects not in acting objects
        for event in global_events:
            for object_id in event.object_ids:
                if object_id not in global_object_ids and object_id not in acting_gaia_ids:
                    #print("Warning: not in Acting Objects: ",event.get_dict())
                    pass

        #initially create all acting objects
        object_dict = {}

        for o_id, object_entry in acting_objects_dict.items():
            object_dict[o_id] = Object(o_id,category =  object_entry['Category'], type = object_entry['Type'], player = object_entry['player'])

        #read events into object_queues
        for event in global_events:
            for object_id in event.object_ids:
                if object_id in acting_objects_dict:
                    object_dict[object_id].add_event(event)
                else:
                    if "Queue" in event.activity:
                        pass
                        #print(event.get_dict()) 

        #read buildings and units into separate lists
        object_types = {'Unit':[], 'Building':[]}

        for o_id, object in object_dict.items():
            object_types[object.category].append(object)


        ## Create intital objects
        init_objects = []
        for p_id in match_data['init_objects']:
            for obj_id, obj in match_data['init_objects'][p_id].items():

                obj['player'] = p_id
                obj['time'] = timedelta(seconds=0)
                obj['type'] = obj['name']

                init_objects.append(obj)

        ## Create OCEL Objects

        OCEL_event_match = []
        OCEL_event_creation = []

        queue_dict = {}

        #create buildings
        build_global, build_creation = execute_builds(global_events)

        OCEL_event_match += build_global
        OCEL_event_creation += build_creation
        


        #create initial creations
        for obj in init_objects:
            OCEL_obj = OCEL_event("Create Initial " + obj['type'], obj['time'], obj['player'], attributes_dict={'Player': [obj['player']]}, init_act_id=obj['object_id'])
            OCEL_event_match.append(OCEL_obj)
            OCEL_event_creation.append(OCEL_obj)

        #execute queues
        for idx, object in enumerate(object_types['Building']):
            queue_global, queue_creation, queue_execution = execute_queue(object, object.player)
            
            #for debugging
            queue_dict[idx] = queue_execution

            OCEL_event_match += queue_global
            OCEL_event_creation += queue_creation 

            

        ## Assign Acting ObjectIDs to OCEL Objects

        idx = 0
        objs = {}

        #sort creation by timestamp
        OCEL_event_creation.sort(key=lambda x: x.ocel_time)
        for event in OCEL_event_creation:

            #Skip research events, stem from Research Age in Queue Buildings but create no objects
            if "Research" in event.ocel_type:
                continue
            
            #Create type by removing the firts 2 words from event.ocel_type
            type = " ".join(event.ocel_type.split(" ")[2:])


            objs[idx] = {'idx': idx,
                    'type':type,
                        'time':event.ocel_time,
                        'player': event.player,
                        'act_id': None }
            
            idx += 1
        #create counter_type, e.g. Villager 1

        counter_dict = {}

        for key, obj in objs.items():
            if obj['type'] not in counter_dict:
                counter_dict[obj['type']] = 1
            else:
                counter_dict[obj['type']] += 1

            objs[key]['c_type'] = obj["type"] + " " + str(counter_dict[obj['type']])



        df = pd.DataFrame(objs).T


        #create type_dict with all objects of a type
        types = df['type'].unique()
        type_dict = {}

        for type in types:
            type_dict[type] = []

        for idx, obj in objs.items():
            type_dict[obj['type']].append(obj)

        ###create acting objects for matching

        act_obj_keys = []
        for key in acting_objects_dict:
            act_obj_keys.append(key)

        act_obj_keys.sort()

        act_objs = {}

        for key in act_obj_keys:
            act_objs[key] = {
                'idx':key,
                'act_type': acting_objects_dict[key]['Type'],
                'time': acting_objects_dict[key]['Time'],
                'player': acting_objects_dict[key]['player']
            }


        #begin matching
        found = False
        fc = 0

        rem_act_objs = [obj for obj in act_objs.values()]
        rem_act_objs.sort(key=lambda x: x['time'])

        unassigned = []

        for act_obj in rem_act_objs:

            type = act_obj["act_type"]
            found = False

            #try to assign object to existing object
            try:
                for obj in type_dict[type]:
                    if obj['act_id'] is None and obj['time'] < act_obj['time'] and obj['player'] == act_obj['player']:
                        obj['act_id'] = act_obj['idx']
                        obj['act_time'] = act_obj['time']

                        found = True
                        fc +=1
                        break

                if not found:
                    unassigned.append(act_obj)

            except:
                unassigned.append(act_obj)


        #print("Match: ",match_id,"Found: ", fc ,  " Unassigned: ", len(unassigned))



        #create global ocel objects reindex later for better readability
        OCEL_obj_match = []

        for type in type_dict:
            for obj in type_dict[type]:
                OCEL_obj_match.append(OCEL_object(obj['c_type'],obj['type'],obj['player'],creation_time= obj['time'],act_id=obj['act_id']))


        #loop over initial events and assign correct id
        for event in OCEL_event_creation:
            if event.init_act_id is not None:
                for obj in OCEL_obj_match:
                    if obj.act_id == event.init_act_id:
                        if obj.ocel_type == "Villager":
                            event.attributes['Villager'].append(obj.ocel_id)
                        
                        elif obj.ocel_type == "Town Center":
                            event.attributes['Building'].append(obj.ocel_id)



        ## assign correct objects to command and complete queue events
        count_dict = {}
        queue_order_dict = {}
        queue_info_dict = {}



        for event in OCEL_event_match:
            if event.queue_info is not None:
                
                #write all events of same object in one list
                if event.queue_info in queue_info_dict:
                    queue_info_dict[event.queue_info].append(event)
                else:
                    queue_info_dict[event.queue_info] = [event]
                
                #for complete events get the correct id
                if "Complete Queue" in event.ocel_type:
                    obj_ident = event.ocel_type.split("Complete Queue ")[1]
                    
                    #get id of object with counter
                    if obj_ident in count_dict:
                        count_dict[obj_ident] += 1 
                    else:
                        count_dict[obj_ident] = 1

                    queue_order_dict[event.queue_info] = f"{obj_ident} {count_dict[obj_ident]}"


        #loop over objects and assign correct id
        for queue_info in queue_info_dict:
            for event in queue_info_dict[queue_info]:
                if "Villager" in event.ocel_type:
                    event.attributes['Villager'].append(queue_order_dict[queue_info])
                #add the next line to include Units in the Object Assignment
                #elif "Research" not in event.ocel_type:
                    #event.attributes['Unit'].append(queue_order_dict[queue_info])

        ## Update ObjectIDs in OCEL Events
        #NOTE issue with the writing of the objects into the OCEL events, since the majority of buildings and units are non acting. 

        #create accesible dict for act_ids listing the OCEL_objects
        ocel_obj_actid = {}

        for obj in OCEL_obj_match:
            ocel_obj_actid[obj.act_id] = obj

        ######## Create all Gather Events with accesible obj_actid
        
        gatherpoint_dict = {}


        #create Gather and Gather point Events
        for event in global_events:

            #identify gather and gather point events and map them to the correct resource
            if "Gather" in event.activity:
                #Filter Out None Gather Points or Building Gather Points
                if event.activity_subtype == None or event.activity_subtype == "None" or event.activity_subtype in base_build_times:
                    gather_target = "None"
                else:
                    try:
                        #aggregate subcategories for robustness
                        if "Tree" in event.activity_subtype:
                            event.activity_subtype = "Tree"
                        if "Fish" in event.activity_subtype:
                            event.activity_subtype = "Fish"
                        if "Bush" in event.activity_subtype:
                            event.activity_subtype = "Bush"
    
                        gather_target = gather_map[event.activity_subtype]

                    except: 
                        print(event.activity_subtype)
                        continue
            else:
                continue

            
            if "Gather Point" in event.activity:
                #add ocel_events
                attribute_dict = {
                        'Player': [event.player_id],
                        'Villager': [],
                        'Building': event.object_ids
                        }
                event_type = "Set Gather Point " + gather_target
                gatherpoint_event = OCEL_event(event_type, event.timestamp, event.player_id, attributes_dict=attribute_dict)
                OCEL_event_match.append(gatherpoint_event)
                
                #fill gatherpoint_dict for lookup
                build_id = event.object_ids[0]

                if build_id not in gatherpoint_dict:
                    gatherpoint_dict[build_id] = []
                bisect.insort(gatherpoint_dict[build_id], (event.timestamp, gather_target))

            elif "Gather " in event.activity:
                try:
                    attribute_dict = {
                            'Player': [event.player_id],
                            'Villager': event.object_ids,
                            'Building': []
                            }
                    event_type = "Gather " + gather_target
                    gather_event = OCEL_event(event_type, event.timestamp, event.player_id, attributes_dict=attribute_dict)
                    OCEL_event_match.append(gather_event)
                except: print(event.get_dict())


        def get_gather_point(build_id, creation_timestamp):
            if build_id not in gatherpoint_dict or not gatherpoint_dict[build_id]:
                return "None"
            
            gather_points = gatherpoint_dict[build_id]
            pos = bisect.bisect_left(gather_points, (creation_timestamp, ''))
            
            if pos == 0:
                return "None"
            else:
                return gather_points[pos - 1][1]
        
        #create Gather Events for all produced vills
        for event in OCEL_event_match:
            if event.ocel_type == "Complete Queue Villager":
                try:
                    time = event.ocel_time
                    build_id = event.attributes['Building'][0]
                    gather_target = get_gather_point(build_id, time)
                    gather_event = OCEL_event("Gather " + gather_target, time, event.player, attributes_dict=event.attributes)
                    OCEL_event_match.append(gather_event)
                except:
                    print("No GP")

    

        #go over all events and overwrite the object lists in the attributes
        for event in OCEL_event_match:
            for key in event.attributes:
                #ignore playerID
                if key == 'Player':
                    continue

                #clean duplicates
                event.attributes[key] = list(set(event.attributes[key]))

                for act_id in event.attributes[key]:
                    #check if numeric - then assume acting ID 
                    if isinstance(act_id, (int, float)) or (isinstance(act_id, str) and act_id.replace('.', '', 1).isdigit()):
                        #replace act_id with ocel_id
                        try:
                            event.attributes[key] = [ocel_obj_actid[act_id].ocel_id for act_id in event.attributes[key]]
                        except:
                            #print("Act ID not found: ", act_id)
                            #print(ocel_obj_actid)
                            pass
                        break 
        

        #loop over all event and replace timestamp from delta to time from match start

        base_time = match_data['match_info']['start_time'] 

        #create object_dict for adressing objects
        OCEL_obj_dict = {}

        for obj in OCEL_obj_match:
            OCEL_obj_dict[obj.ocel_id] = obj


        #Sort events by timestamp
        OCEL_event_match.sort(key=lambda x: x.ocel_time)


        #assign events to objects
        for event in OCEL_event_match:
            new_time = base_time + event.ocel_time
            event.ocel_time = new_time
            
            for attr in event.attributes:
                if attr != 'Player':
                    for obj_id in event.attributes[attr]:
                        try:
                            OCEL_obj_dict[obj_id].events.append(event)
                        except:
                            #print("Failed at ", obj_id)
                            #print(event)
                            pass

        #define creation time for objects
        for object in OCEL_obj_match:
            try:
                init_event = object.events[0]

                #print(object.ocel_id, object.ocel_type,init_event.ocel_time, init_event.ocel_type)
            except:
                #print("No events for ", object.ocel_id)
                pass

        return OCEL_event_match, OCEL_obj_match, queue_dict, match_data['match_info']


    ####################################### Global Run ########################################



    rename_attributes = ["Villager","Building"]


    succ = 0
    fail = 0


    event_types = set()
    object_types = set()

    OCEL_event_global = []
    OCEL_obj_global = []

    #extract match log
    for match_id in match_ids:

        try:

            filepath = recordfile_path + f'/AgeIIDE_Replay_{match_id}.aoe2record'

            OCEL_event_match, OCEL_obj_match, queue_dict, match_info = oc_log_from_record(match_id,filepath)

            OCEL_event_match.sort(key=lambda x: x.ocel_time)

            succ +=1

            #print("Succ_id: ", match_id)

        except:
            fail +=1
            continue

        #get player info 
        match_players = set()   
        for event in OCEL_event_match:
            match_players.add(event.player)

        #define match_name
        match_OCELID = f'M{match_id}'

        #update events matchdependent
        for idx, event in enumerate(OCEL_event_match):
            event.event_id = f'e_M{match_id}_{idx}'
            event.attributes['Match'] = [match_OCELID]
            event.attributes['Session'] = [f'S{match_id}_{event.player}']
            event.ocel_type = clean_special(event.ocel_type)
            event_types.add(event.ocel_type)
            
            #rename objects in events
            for attr in rename_attributes:
                event.attributes[attr] = [rename_object(obj, match_id) for obj in event.attributes[attr]]

            event.attributes['Player'] = [f'P{player_id}' for player_id in event.attributes['Player']]


        #update objects matchdependent
        for idx, obj in enumerate(OCEL_obj_match):
            obj.ocel_id = rename_object(obj.ocel_id, match_id)

        #create match objects
        match_object = OCEL_object(match_OCELID,"Match", None)
        OCEL_obj_match.append(match_object)

        match_attributes = {
            'map': match_info['map'],
            'duration': match_info['duration'],
            'player': [],
            'session': []
        }

        #create player and session objects NOTE add 
        for p_id in match_players:
            player_object = OCEL_object(f'P{p_id}',"Player", None)
            OCEL_obj_match.append(player_object)
            match_attributes['player'].append(player_object.ocel_id)

            #check for winner
            if p_id in match_info['winner_ids']:
                session_attributes = {'win': 1}
            else:
                session_attributes = {'win': 0}
                
            
            session_object = OCEL_object(f'S{match_id}_{p_id}',"Session", None,attributes=session_attributes)
            OCEL_obj_match.append(session_object)
            match_attributes['session'].append(session_object.ocel_id)

        #annotate match_object with player,map and session
        match_object.attributes = match_attributes

        
        #add object types
        for obj in OCEL_obj_match:
            obj.ocel_type = clean_special(obj.ocel_type)
            object_types.add(obj.ocel_type)

        #write into global objects
        OCEL_event_global += OCEL_event_match
        OCEL_obj_global += OCEL_obj_match


        if succ >= goal:
            break
        

    ####################################### OCEL Table Export ########################################
    print(f"{succ} Matches processed successfully. Starting OCEL export.")

    #create event dfs
    tables = {}
    tables['event_map_type'] = []
    tables['object_map_type'] = []
    map_type_dict = {}


    #create empty table lists
    table_names = ['event','event_object','object','object_object']

    for et in event_types:
        #remove spaces and hyphens from et
        et_clean = clean_str(et)
        et = clean_special(et)
        #add to table_name
        table_names.append(f'event_{et_clean}')
        #safe mapping
        tables['event_map_type'].append({'ocel_type':et, 'ocel_type_map': et_clean})
        map_type_dict[et] = f'event_{et_clean}'

    for ot in object_types:
        #remove spaces and hyphens from ot
        ot_clean = clean_str(ot)
        ot = clean_special(ot)
        tn = f'object_{ot_clean}'
        #add to table_name
        table_names.append(tn)
        #safe mapping
        tables['object_map_type'].append({'ocel_type':ot, 'ocel_type_map': ot_clean})
        map_type_dict[ot] = tn

    #generate empty tables
    for tn in table_names:
        tables[tn] = []

    table_names+= ['event_map_type','object_map_type']


    for event in OCEL_event_global:
        #write events into event table
        tables['event'].append({'ocel_id': event.event_id, 'ocel_type': event.ocel_type})

        #write events into types tables
        tables[map_type_dict[event.ocel_type]].append({'ocel_id': event.event_id, 'ocel_time': event_format_timestamps(event.ocel_time)})

        #fill event objects table
        for attr in event.attributes:
            for obj in event.attributes[attr]:
                tables['event_object'].append({'ocel_event_id': event.event_id, 'ocel_object_id': obj, 'ocel_qualifier': ""})
        
        #fill object to object table
        temp_obj_list = set()
        for attr in event.attributes:
            for obj in event.attributes[attr]:
                temp_obj_list.add(obj)
        temp_obj_list = list(temp_obj_list)


            # Convert set to list and sort it with custom logic
        def custom_sort_key(x):
            if x.startswith('P'):
                return (0, x)
            elif x.startswith('S'):
                return (2, x)
            elif x.startswith('M'):
                return (3, x)
            else:
                return (1, x)
    
        temp_obj_list = sorted(temp_obj_list, key=custom_sort_key)
        
        # Create all 2-combinations of the sorted object list
        obj_combinations = list(combinations(temp_obj_list, 2))

        #define o2o qualifiers
        o2o_relations = {}
        
    
        for comb in obj_combinations:
            tables['object_object'].append({'ocel_source_id': comb[0], 'ocel_target_id': comb[1], 'ocel_qualifier': ""})


    for obj in OCEL_obj_global:
        #write objects into object table
        tables['object'].append({'ocel_id': obj.ocel_id, 'ocel_type': obj.ocel_type})

        #write objects into types tables (can be extended)
        tables[map_type_dict[obj.ocel_type]].append({'ocel_id': obj.ocel_id,'ocel_time': object_format_timestamps(obj.creation_time), 'ocel_change_field': None})



    #convert tables
    for tn in table_names: 
        tables[tn] = pd.DataFrame(tables[tn])
        #print(tables[tn].head())

    #in every table remove all elements within remove list for all column which are not ocel_time
    remove_list = ['.','-',':','(',')']

    for tn in table_names:
        for col in tables[tn].columns:
            if col != 'ocel_time':
                for rem in remove_list:
                    tables[tn][col] = tables[tn][col].str.replace(rem, '')

    tn_lengths = []

    #drop duplicates in all tables
    for tn in table_names:
        prev = len(tables[tn])
        tables[tn] = tables[tn].drop_duplicates()
        tn_lengths.append({'tn':tn, 'prev':prev,'after': len(tables[tn])})

####################################### SQL Export ########################################
    
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    for tn in table_names:
        tables[tn].to_sql(tn, conn, index=False)
    conn.close()

    #return metrics

    if succ < goal:
        print("Only ", succ, " matches exported sucessfully.")

    else:
        print("Export of ", succ, " matches completed.")

    return (succ, fail,tables)


