
#!/usr/bin/env python3

import pprint
import collections

from Chinook_Python import *
from Chinook_Python import InvoiceLine
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
    
print("-----------------------CROSS JOIN--------------------------------")

pprint.pprint(
    project(
    select(
    select(
    cross(Employee,
    project(
    select(
    cross(Customer,
    rename(
    project(
    select(
    cross(Invoice,
    rename(
    project(
    select(
    cross(InvoiceLine,
    rename(
    project(
            select(
                cross(
                    Track,
                    rename(MediaType,['MId','MName']),
                ),lambda t: t.MediaTypeId == t.MId
            ),['TrackId','MName']
    ),['TId','MName']
)
),lambda t: t.TrackId == t.TId
),['InvoiceId','MName']
),['Iid','MName']
)
),lambda t: t.InvoiceId == t.Iid
),['CustomerId','MName']
),['CId','MName']
)
),lambda t:t.CustomerId == t.CId
),['SupportRepId','MName']
)
),lambda t: t.EmployeeId == t.SupportRepId
),lambda t: t.MName == 'Purchased AAC audio file' 
), ['FirstName','LastName']
)
)
print("Total Tuples Processed:",count)
count = 0

print("------------------SELECT BEFORE THETA JOIN-----------------------")

pprint.pprint(
    project(
    theta_join(Employee,
    project(
    theta_join(Customer,
    rename(
    project(
    theta_join(Invoice,
    rename(
    project(
    theta_join(InvoiceLine,
    rename(
    project(
        theta_join(
            Track,
            rename(
                select(MediaType, lambda t: t.Name == 'Purchased AAC audio file'),
                ['MId', 'MName']
            ),
            lambda t1, t2: t1.MediaTypeId == t2.MId
        ),['TrackId']
    ),['TId']
    ), lambda t1,t2: t1.TrackId == t2.TId
),['InvoiceId']
),['Iid']
), lambda t1,t2: t1.InvoiceId == t2.Iid
),['CustomerId'] 
),['CId']
), lambda t1,t2: t1.CustomerId == t2.CId
),['SupportRepId']
), lambda t1,t2: t1.EmployeeId == t2.SupportRepId
),['FirstName','LastName']
)
)

print("Total Tuples Processed:",count)
count = 0

print("-----------------SELECT AFTER THETA JOIN-------------------------")

pprint.pprint(
    project(
    select(
    theta_join(Employee,
    project(
    theta_join(Customer,
    rename(
    project(
    theta_join(Invoice,
    rename(
    project(
    theta_join(InvoiceLine,
    rename(
    project(
        theta_join(
            Track,
            rename(MediaType,['MId','MName']),
            lambda t1, t2: t1.MediaTypeId == t2.MId
        ),['TrackId','MName']
    ),['TId','MName']
    ), lambda t1,t2: t1.TrackId == t2.TId
),['InvoiceId','MName']
),['Iid','MName']
), lambda t1,t2: t1.InvoiceId == t2.Iid
),['CustomerId','MName'] 
),['CId','MName']
), lambda t1,t2: t1.CustomerId == t2.CId
),['SupportRepId','MName']
),lambda t1,t2: t1.EmployeeId == t2.SupportRepId
),lambda t1: t1.MName == 'Purchased AAC audio file'
),['FirstName','LastName']
)
)
print("Total Tuples Processed:",count)
count = 0

print("----------------------NATURAL JOIN-----------------------------")

pprint.pprint(
            project(
            natural_join(Employee,
            project(
            rename(
            project(
            natural_join(Customer,
            project(
            natural_join(Invoice,
            project(
            natural_join(InvoiceLine,
            project(
            natural_join(Track,
            project(
            select(MediaType, lambda t: t.Name == 'Purchased AAC audio file'),
            ['MediaTypeId'],
        )      
),['TrackId']
)           
),['InvoiceLineId','InvoiceId']
)
),['InvoiceId','CustomerId']
)
),['CustomerId','SupportRepId']
),['CustomerId','EmployeeId']
),['EmployeeId']
)
),['FirstName','LastName']
)
)
print("Total Tuples Processed:",count)





