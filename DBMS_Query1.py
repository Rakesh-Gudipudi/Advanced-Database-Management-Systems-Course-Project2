
#!/usr/bin/env python3

import pprint
import collections

from Chinook_Python import *
count = 0

def project(relation, columns):
    global count
    project_list = []
    Result = collections.namedtuple('Result',columns)
    final_set = set([tuple(getattr(row, column) for column in columns)
                for row in relation])
    for i in final_set:
        project_list.append(Result(*i))
    count += len(project_list)
    return set(project_list)

def select(relation, predicate):
    global count
    select_list = []
    for i in relation:
        if(predicate(i)):
            select_list.append(i)
    count += len(select_list)
    return set(select_list)

def rename(relation, new_columns=None, new_relation=None):
    global count
    new_relation = new_relation if new_relation!=None else 'Result'
    rename_list = [] 
    new_relation = collections.namedtuple(new_relation,new_columns)
    for i in relation:
        rename_list.append(new_relation(*i))
    count+=len(rename_list)
    return set(rename_list)

def cross(relation1, relation2):
    global count
    t1 = next(iter(relation1))
    t2 = next(iter(relation2))
    Result = collections.namedtuple('Result', t1._fields + t2._fields)
    cross_list =[]
    for i in relation1:
        for j in relation2:
            cross_list.append(Result(*(i+j)))
    count+=len(cross_list)
    return (set(cross_list))

def theta_join(relation1, relation2, predicate):
    global count
    t1 = next(iter(relation1))
    t2 = next(iter(relation2))
    Result = collections.namedtuple('Result', t1._fields + t2._fields)
    theta_list =[]
    for i in relation1:
        for j in relation2:
            if(predicate(i,j)):
                theta_list.append(Result(*(i+j)))
    count += len(theta_list)
    return (set(theta_list))

def natural_join(relation1, relation2):
    global count
    #Creating a list of attributes in Relation2 without the common fields
    lo=[]
    relation2_sorted_list = []
    t1 = next(iter(relation1))
    t2 = next(iter(relation2))  
    set_common_field = set(t1._fields).intersection(set(t2._fields))
    set_tuples_left = set(t2._fields) - set(t1._fields)
    for i in relation2:
        for j in i:
            if(getattr(i,next(iter(set_common_field))) != j):
                lo.append(j)
        relation2_sorted_list.append(tuple(lo))
        lo=[] 
    #Creating a result set with combining two relations with one attribute which is common both the relations
    lx = []
    Test = collections.namedtuple('Test',list(set_tuples_left)) 
    for p in relation2_sorted_list:
        lx.append(Test(*p))
    natural_list = []  
    Result = collections.namedtuple('Result', t1._fields + tuple(set_tuples_left))
    for i in relation1:
        for j in relation2:
            if(getattr(i,next(iter(set_common_field))) == getattr(j,next(iter(set_common_field)))):
                for k in lx:
                    natural_list.append(Result(*(i+k)))
    count += len(natural_list)
    return set(natural_list)
    
print("--------------------CROSS JOIN-----------------------------")

pprint.pprint(
    project(
        select(
            select(
                cross(
                    Album,
                    rename(Artist, ['Id', 'Name'])
                ),
                lambda t: t.ArtistId == t.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)
print("Total Tuples Processed:",count)
count = 0
print("-------------SELECT BEFORE THETA JOIN----------------------")

pprint.pprint(
    project(
        select(
            theta_join(
                Album,
                rename(Artist, ['Id', 'Name']),
                lambda t1, t2: t1.ArtistId == t2.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)
print("Total Tuples Processed:",count)
count = 0

print("--------------SELECT AFTER THETA JOIN----------------------")

pprint.pprint(
    project(
        theta_join(
            Album,
            rename(
                select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers'),
                ['Id', 'Name'],
            ),
            lambda t1, t2: t1.ArtistId == t2.Id
        ),['Title']
    )
)
print("Total Tuples Processed:",count)
count = 0

print("--------------NATURAL JOIN----------------------")
pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )

)
print("Total Tuples Processed:",count)