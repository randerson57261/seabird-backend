from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from sqlalchemy import func
import os
import pandas as pd
#### Setup ##########################

app = Flask(__name__)


app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
#app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://ryan:4520@localhost/seabird"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CORS_HEADERS'] = 'Access-Control-Allow-Origin'

db = SQLAlchemy(app)
api = Api(app)

cors = CORS(app, resources={r"/results": {"origins": "*"}, r"/getAreas": {"origins": "*"}, r"/getCblocks": {"origins": "*"}, r"/getGroups": {"origins": "*"},r"/getYears": {"origins": "*"},r"/getSpecies": {"origins": "*"}, r"/mapResults": {"origins": "*"},  r"/rrhresults": {"origins": "*"},r"/disturbResults": {"origins": "*"}});

################ Models ##################, 
class b_results(db.Model):

    __tablename__ = 'b_results'

    index = db.Column(db.Integer, primary_key = True)
    year = db.Column(db.Integer)
    group_name = db.Column(db.Text)
    survey_area = db.Column(db.Text)
    count_block = db.Column(db.Integer)
    species = db.Column(db.Text)
    population = db.Column(db.Float)

    def __init__(self, index, year, group_name, survey_area, count_block, species, population):
        self.index = index
        self.year = year
        self.group_name = group_name
        self.survey_area = survey_area
        self.count_block = count_block
        self.species = species
        self.population = population

class rrh_results(db.Model):

    __tablename__ = 'rrh_results'

    index = db.Column(db.Integer, primary_key = True)
    year = db.Column(db.Integer)
    group_name = db.Column(db.Text)
    survey_area = db.Column(db.Text)
    count_block = db.Column(db.Integer)
    species = db.Column(db.Text)
    rrh = db.Column(db.Float)

    def __init__(self, index, year, group_name, survey_area, count_block, species, rrh):
        self.index = index
        self.year = year
        self.group_name = group_name
        self.survey_area = survey_area
        self.count_block = count_block
        self.species = species
        self.rrh = rrh

class Disturb_Results(db.Model):

    __tablename__ = 'Disturb_Results'

    index = db.Column(db.Integer, primary_key = True)
    group_name = db.Column(db.Text)
    survey_area = db.Column(db.Text)
    year = db.Column(db.Integer)
    disturbs_per_day = db.Column(db.Float)

    def __init__(self, index, group_name, survey_area, year, disturbs_per_day):
        self.index = index
        self.group_name = group_name
        self.survey_area = survey_area
        self.year = year
        self.disturbs_per_day = disturbs_per_day

########################### CRUD ##########
#############################################

#########  Disturbance
# Get data for plot
class disturbResults(Resource): 
  
    def get(self): 
        #Get value from drop down selection boxes
        arg_group = request.args['group']

        #convert arguments to conditions
        group_cond, _ , _ = argCondition('disturb',arg_group)

        #Get list of unique areas in database
        areas= db.session.query(Disturb_Results.survey_area.distinct().label("survey_area")).all()
       
        #loop through each species and extract sums of species count (with group/area/cblock filters)
        graph_data = []
        for area in areas:
            records = db.session.query(Disturb_Results.year, Disturb_Results.disturbs_per_day)\
                .filter(group_cond, Disturb_Results.survey_area==area)\
                .order_by(Disturb_Results.year)\
                .all()
            
            #extract years and counts to seperate lists, append to 'graph data' in json format
            if records:
                years = [item[0] for item in records]
                rates = [item[1] for item in records]
                graph_data.append({'name':area[0],'x':years, 'y':rates})

        #return json or error
        if graph_data:
            return jsonify(graph_data)
        else:
            return {'Error':None}, 404 


##########   Population
# Get data for population plot 
class retrieveResults(Resource): 
  
    def get(self): 
        #Get value from drop down selection boxes
        arg_group = request.args['group']
        arg_area = request.args['area']
        arg_cblock = request.args['cblock']

        #convert arguments to conditions
        group_cond, area_cond, cblock_cond = argCondition('pop', arg_group, arg_area, arg_cblock)

        #Get list of unique species in database
        species = db.session.query(b_results.species.distinct().label("species")).all()

        #loop through each species and extract sums of species count (with group/area/cblock filters)
        graph_data = []
        for spe in species:
            aggregated = db.session.query(b_results.year, func.sum(b_results.population))\
                .filter(group_cond, area_cond, cblock_cond, b_results.species==spe)\
                .group_by(b_results.year)\
                .order_by(b_results.year)\
                .all()
            
            #extract years and counts to seperate lists, append to 'graph data' in json format
            years = [item[0] for item in aggregated]
            counts = [item[1] for item in aggregated]
            graph_data.append({'name':spe[0],'x':years, 'y':counts})

        #return json or error
        if aggregated:
            return jsonify(graph_data)
        else:
            return {'Error':None}, 404 

##########   RRH
#Get data for roosting/rafting/hauled out
class retrieveRRHresults(Resource): 
  
    def get(self): 
        #Get value from drop down selection boxes
        arg_group = request.args['group']
        arg_area = request.args['area']
        arg_cblock = request.args['cblock']

        #convert arguments to conditions
        group_cond, area_cond, cblock_cond = argCondition('rrh', arg_group, arg_area, arg_cblock)

        #Get list of unique species in database
        species = db.session.query(rrh_results.species.distinct().label("species")).all()

        #loop through each species and extract sums of species count (with group/area/cblock filters)
        graph_data = []
        for spe in species:
            aggregated = db.session.query(rrh_results.year, func.sum(rrh_results.rrh))\
                .filter(group_cond, area_cond, cblock_cond, rrh_results.species==spe)\
                .group_by(rrh_results.year)\
                .order_by(rrh_results.year)\
                .all()
            
            #extract years and counts to seperate lists, append to 'graph data' in json format
            years = [item[0] for item in aggregated]
            counts = [item[1] for item in aggregated]
            graph_data.append({'name':spe[0],'x':years, 'y':counts})

        #return json or error
        if aggregated:
            return jsonify(graph_data)
        else:
            return {'Error':None}, 404 

############# Map
#Get data for map
class retrieveMapResults(Resource):
    def get(self):
        #Get variables
        arg_year = request.args['year']
        arg_species = request.args['species']

        #convert to conditional
        year_cond, species_cond = MapArgCondition(arg_year, arg_species)   

        dbResults = db.session.query(b_results.group_name, b_results.survey_area, b_results.count_block, func.sum(b_results.population))\
            .filter(year_cond, species_cond)\
            .group_by(b_results.group_name, b_results.survey_area, b_results.count_block)\
            .all()

        mapData = []
        for item in dbResults:
            group = item[0]
            area = item[1]
            cblock = item[2]
            population = item[3]

            x, y = getCoords(group, area, cblock)

            mapData.append({'group':group,'area':area, 'cblock':cblock, 'population':population, 'x':x, 'y':y})
        
        #return json or error
        if mapData:
            return jsonify(mapData)
        else:
            return {'Error':None}, 404 


##### Data for drop downs
class getGroups(Resource): 
    def get(self): 
        mode = request.args['mode']
        if mode == 'pop':
            table = b_results
        elif mode == 'disturb':
            table = Disturb_Results
        elif mode == 'rrh':
            table = rrh_results

        #Get list of unique species in database
        groups = db.session.query(table.group_name.distinct().label("group_name"))\
            .order_by(table.group_name)\
            .all()

        if groups:
            return jsonify({'groups': [item[0] for item in groups]})
        else:
            return {'Error': 'No Areas'}, 404

class getAreas(Resource): 
    def get(self): 
        #retrieve arguments
        arg_group = request.args['group']

        mode = request.args['mode']
        if mode == 'pop':
            table = b_results
        elif mode == 'disturb':
            table = Disturb_Results
        elif mode == 'rrh':
            table = rrh_results

        #convert arguments to conditions
        group_cond, area_cond, cblock_cond = argCondition(mode, arg_group)

        #Get list of unique survey areas in database
        areas = db.session.query(table.survey_area.distinct().label("survey_area"))\
            .filter(group_cond)\
            .order_by(table.survey_area)\
            .all()

        if areas:
            return jsonify({'areas': [item[0] for item in areas]})
        else:
            return {'Error': 'No Areas'}, 404

class getCblocks(Resource): 
    def get(self): 
        #retrieve arguments
        arg_group = request.args['group']
        arg_area = request.args['area']
        mode = request.args['mode']

        mode = request.args['mode']
        if mode == 'pop':
            table = b_results
        elif mode == 'disturb':
            table = Disturb_Results
        elif mode == 'rrh':
            table = rrh_results

        #convert arguments to conditions
        group_cond, area_cond, cblock_cond = argCondition(mode, arg_group, arg_area)

        #Get list of unique species in database
        cblocks = db.session.query(table.count_block.distinct().label("count_block"))\
            .filter(group_cond, area_cond)\
            .order_by(table.count_block)\
            .all()

        if cblocks:
            return jsonify({'cblocks': [item[0] for item in cblocks]})
        else:
            return {'Error': 'No Cblocks'}, 404

class getYears(Resource): 
    def get(self): 
        #Get list of unique species in database
        years = db.session.query(b_results.year.distinct().label("year"))\
            .order_by(b_results.year)\
            .all()

        if years:
            return jsonify({'years': [item[0] for item in years]})
        else:
            return {'Error': 'No Years'}, 404

class getSpecies(Resource): 
    def get(self): 
        #retrieve arguments
        year = request.args['year']

        #convert arguments to conditions
        year_cond, species_cond = MapArgCondition(year)

        #Get list of unique species in database
        species = db.session.query(b_results.species.distinct().label("species"))\
            .filter(year_cond)\
            .order_by(b_results.species)\
            .all()

        if species:
            return jsonify({'species': [item[0] for item in species]})
        else:
            return {'Error': 'No Species'}, 404

####### Helper functions ############
locations = pd.read_pickle('locations_v2021_0126')
def getCoords(group, area, cblock):
    cords = locations.loc[(locations.group == group) & (locations.area == area) & (locations.cblock==cblock),['x','y']]
    if not cords.empty:
        return cords.values[0][0], cords.values[0][1]
    else:
        
        return 41.085055, -124.284621

def argCondition(mode, arg_group, arg_area='all', arg_cblock='all'):
    #Pull from populationm, rrh, or disturbance table...
    if mode == 'pop':
        table = b_results
    elif mode == 'disturb':
        table = Disturb_Results
    elif mode == 'rrh':
        table = rrh_results

    #Transform form value 'all' to conditional
    if arg_group == 'all':
        group_cond = True
    else:
        group_cond = (table.group_name == arg_group)

    if arg_area == 'all':
        area_cond = True
    else:
        area_cond = (table.survey_area == arg_area)

    if arg_cblock == 'all':
        cblock_cond = True
    else:
        cblock_cond = (table.count_block == arg_cblock)

    return group_cond, area_cond, cblock_cond

def MapArgCondition(year, species='all'):
    #Transform form value 'all' to conditional
    if year== 'all':
        year_cond = True
    else:
        year_cond = (b_results.year == year)

    if species== 'all':
        species_cond = True
    else:
        species_cond = (b_results.species == species)

    return year_cond, species_cond

############# Resources #################
api.add_resource(retrieveResults, '/results')
api.add_resource(retrieveRRHresults, '/rrhresults')
api.add_resource(getGroups, '/getGroups')
api.add_resource(getAreas, '/getAreas')
api.add_resource(getCblocks, '/getCblocks')
api.add_resource(getYears, '/getYears')
api.add_resource(getSpecies, '/getSpecies')
api.add_resource(retrieveMapResults, '/mapResults')
api.add_resource(disturbResults, '/disturbResults')

if __name__=='__main__':
    app.run()