# This is a reference implementation for consuming the new Makindo "Persons" API
# to generate SQL to run against InfoUSA.  In other words, consider this file
# a collection of code snippets that may be useful in your efforts to consume
# the Makindo Persons API
#
# The goal is to figure out which of the Makindo-provided people
# are "found" (exactly one result), "ambiguous" (two or
# more results), or "missing" (zero rules), and ship that conclusion back so that
# the Makindo systems can begin associating "found" people with surveys.
#
# This script may be used as a reference, for inspiration, or may be altered to
# plug additional business requirements.
#
# Many thanks to Rachel@Haystaq & team for the initial code. And apologies for any Python faux pas

import requests
import json
import sys
import getopt
import time
import MySQLdb
from collections import defaultdict

# Haystaq's key
GLOB_CONST_h = {"Authorization":"Token token=\"a2eb02a8-8766-4e17-adf6-766c671050c3\"","Accept":"application/json","Content-Type":"application/json"}
GLOB_CONST_url = "https://api.makindo.io/persons"

def is_number(p_input):
  #self evident
  try:
    float(p_input)
    return True
  except ValueError:
    return False

def blank_ques(p_input):
  #some of the variables like max_age come back as ? which messes with the mysql
  if '?' == p_input:
    p_input = ''
  return p_input

def clean_string(p_input):

  if None == p_input:
    l_return_val = ''
  elif isinstance(p_input, str):
    p_input = p_input.encode('latin-1', 'ignore')
    l_return_val = p_input.strip()
    p_input = blank_ques(p_input)
  elif isinstance(p_input, unicode):
    p_input = p_input.encode('latin-1', 'ignore')
    l_return_val = p_input.strip()
    p_input = blank_ques(p_input)
  elif is_number(p_input):
    l_return_val = str(p_input)
  else:
    l_return_val = ''
  #print "l_return_val = ", l_return_val
  return l_return_val

#placeholder function to fill in with odbc code
# make sure to update the return value to include the external_id and numnber of results
def do_sql(sql):
  print (sql)
  return 0


# convenience function to escape an array for sql
def set_to_sql(the_set):
  if(len(the_set) == 0):
    return ""

  tmplist = []

  for element in the_set:
    tmplist.append("'"+MySQLdb.escape_string(clean_string(element.encode('ascii','ignore')))+"'")

  return ",".join(tmplist)


# this function takes a makindo person and attempts to match it to infousa.
#
def generate_sql(p_cur, p_state, p_city, p_firstname, p_lastname, p_other_names, p_other_locations, p_mysql_errors):


  l_match_type = "ambigous"  # ToDo Rachel -- we really need a "failure" type
  l_person_id = 0


  # start by running sql to query the "name" and "location" against infousa.
  # if one result is found, we're done, "found"

  if( len(p_firstname)==0 or len(p_lastname)==0):
    # print "missing name, aborting"
    return ["failed", p_cur, "no name"]

  # if there's no state associated with profile, try to derive it from alternates
  if(p_state == '' or p_state is None):
    #print "No state, deriving."

    # if we have no alternates abort
    if(len(p_other_locations)==0 ):
      return ["failed", p_cur, "no state"]

    for loc in p_other_locations:
      if(loc["country"] != "United States"):
        continue;
      # check if there's a city match between profile and an alternate
      if(len(loc["state"]) and loc["city"] == p_city and p_city is not None):
        p_state = loc["state"]
        break
      # else just pick one
      else:
        p_state = loc["state"]

    #print "Now state is ", p_state

    if(p_state == '' or p_state is None):
      return ["failed", p_cur, "no state"]

  # now we have a state, moving on

  sql = ''

  if p_city == '' or p_city is None:
    sql = "select individualid from iusa_2013."+p_state+"_indiv_raw where firstname ='"+MySQLdb.escape_string(firstname)+"' and lastname ='"+MySQLdb.escape_string(p_lastname)+"'"
  else:
    sql = "select individualid from iusa_2013."+p_state+"_indiv_raw where firstname ='"+MySQLdb.escape_string(p_firstname)+"' and lastname ='"+MySQLdb.escape_string(p_lastname)+"' and city= '"+MySQLdb.escape_string(p_city)+"'"

  # so, now we try to find the person based on name and location:

  print "**"+ p_firstname+" "+p_lastname+", "+p_city+", "+p_state
  numresult = do_sql(sql)

  # if ONE result is found, stop! This is where you return a "found" status to Makindo,
  # no need to parse the additional locations.
  if(numresult==1):
    return ["found",p_cur, ""]
  elif(numresult > 1):
    return ["ambiguous",p_cur, ""]
  else:
    # build us a structure in which a list of cities is
    # associated with each state
    if len(p_other_locations) > 0:

      locs = defaultdict(list)
      st = ''
      ci = ''
      for loc in p_other_locations:
         if(loc["city"] is not None):
           st = loc["state"]
           ci = loc["city"]
           locs[st].append(ci)


    if len(p_other_names) > 0:

        i = 0
        personallist = [p_firstname]
        familylist = [p_lastname]

        while i < len(p_other_names):
          other_personal = p_other_names[i]['personal']
          if(other_personal is not None):
            personallist.append(other_personal)
          other_family = p_other_names[i]['family']
          if(other_family is not None):
            familylist.append(other_family)
          i += 1

        #print  ",".join(set(familylist))
        #print  ",".join(set(personallist))


    # now we can loop through possible states and build our sql
    for this_state in locs:
       cities = locs[this_state]

       sql =  ("SELECT individualid FROM iusa_2013."+ this_state +"_indiv_raw WHERE "+
              " firstname IN("+ set_to_sql(set(personallist)) +") AND "+
              " lastname IN("+ set_to_sql(set(familylist)) +") AND "+
              " city IN("+ set_to_sql(set(cities)) +")")
       do_sql(sql)

       if(numresult == 1):
         return ["found",p_cur, ""]
         return ["found",p_cur, ""]

#declare variables
mysql_errors = 0
makindo_errors = 0
offset = 0

starttime = time.time()


match_type = ''
person_id = ''
makindo_put = ''

r = requests.get(GLOB_CONST_url, headers = GLOB_CONST_h, verify=False)
if r.status_code is not 200:
  print r.status_code
  sys.exit(r.status_code)

d = json.loads(r.text)

persons = d['persons']

for record in persons:
  #print "Record>>"
  #print record
  #print "<<End Record"
  #print "start request and return strings -- time = ", time.time() - starttime
  makindoid = clean_string(record['id'])

  if(record['name'] is None):
    continue
    firstname = clean_string(record['name'].split()[0])
    middlename = clean_string(" ".join(record['name'].split()[1:-1]))
    lastname = clean_string(record['name'].split()[-1])
    age_min = clean_string(record['age']['minimum'])
    age_max = clean_string(record['age']['maximum'])
    country = clean_string(record['location']['country'])
    state = record['location']['state']
    city = clean_string(record['location']['city'])
    other_locations = record['locations']
    other_names = record['names']

# not sure about these return values
match_type, person_id, mysql_errors = generate_sql(makindoid, state, city, firstname, lastname, other_names, other_locations, mysql_errors


