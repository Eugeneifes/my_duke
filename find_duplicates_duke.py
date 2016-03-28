# -*- coding: utf-8 -*-
from pskov.extractor.job_processors.base_job import BaseJob
from pskov.rest.handlers.idsgroup_handler import IdsGroupHandler
from pskov.utils.term_manager_client import TermManagerClient
from pskov.utils.connection_manager import ConnectionManager
from tempfile import mkdtemp
import csv
from lxml import etree
from xml.etree.ElementTree import SubElement
import shutil
import subprocess
import json
from pskov.utils import data_utils
import datetime

class FindDuplicatesDukeJob(BaseJob):

    def check_job(self, job):
        return job.get("action") == "find_duplicates_duke"

    def process_job(self, job):

        documents = ConnectionManager.elastic_raw.search(query=job.get("query"))

        fields = json.loads(job.get("fields")).keys()
        print("\n")
        print("Our fields:")
        for field in fields:
            print field
        print("\n")

        doc_type = job.get("object_type")
        print("Working with '%s' documents"% doc_type)
        print("\n")

        #создаем временную директорию
        dirpath = mkdtemp()
        print ("Temporary dir '%s' is created" % dirpath)
        print("\n")


        self.make_csv(fields, dirpath, documents)

        #формируем вектор для конфигурационного файла
        duke_fields = []
        for field in fields:
            tmp = []
            tmp.append(field)
            tmp.append("no.priv.garshol.duke.cleaners.LowerCaseNormalizeCleaner")
            tmp.append("no.priv.garshol.duke.comparators.Levenshtein")
            tmp.append("0.1")
            tmp.append("0.7")
            duke_fields.append(tmp)


        """
        #считаем популярные поля
        importance = {}
        for field in fields[1:]:
            for document in documents:
                str_document = json.dumps(document)
                dict_document = json.loads(str_document)
                if data_utils.dict_get(dict_document, field) not in [None, []]:
                    try:
                        importance[field] += 1
                    except:
                        importance[field] = 1

        for field in importance.keys():
            print field, importance[field]
        """

        #опорные поля, по которым будут формироваться кандидаты для сравнения
        #тип документа, с которым работаем (от типа зависит набор ключевых полей для Duke)
        if job.get("object_type") == "persons":
            target_fields = ["facets.person.Name",
                          "facets.person.birthday",
                          "facets.person.personal.reg_address_str",
                          "facets.person.personal.region"]


        if job.get("object_type") == "powerplants":
            target_fields = ["facets.plant.name",
                             "facets.geo.country",
                             "facets.geo.state",
                             "facets.plant.owner"]


        self.xml_config(duke_fields, target_fields, dirpath, dirpath+"/database_csv.csv")
        self.duke(dirpath)
        merge = self.parse_output(dirpath+"/out_file.txt")
        self.to_mongo(merge, fields, doc_type, dirpath)


    def make_csv(self, fields, dirpath, documents):

        #открываем на запись базу
        with open("%s/database_csv.csv" % dirpath, "w") as database:
            writer = csv.writer(database)
            fields.insert(0, "id")
            writer.writerow(fields)

            #пишем сами объекты
            for document in documents:
                row = []
                row.append(document.docid)
                str_doc = json.dumps(document)
                dict_doc = json.loads(str_doc)

                for field in fields[1:]:

                    if data_utils.dict_get(dict_doc, field) != None:
                        row.append(data_utils.dict_get(dict_doc, field))
                    else:
                        row.append(None)

                writer.writerow(row)



    #генерация конфигурационного файла XML
    def xml_config(self, fields, target_fields, dirpath, csv_database):

        root = etree.Element('duke')
        schema = SubElement(root, 'schema')

        threshold = etree.Element('threshold')
        threshold.text = "0.9"
        schema.append(threshold)

        maybethreshold = etree.Element('maybe-threshold')
        maybethreshold.text = "0.5"
        schema.append(maybethreshold)


        for elem in fields:

            if elem[0] == "id":

                property = etree.Element('property')
                property.attrib['type'] = "id"
                name = SubElement(property, 'name')
                name.text = elem[0]
                schema.append(property)
            else:

                property = etree.Element('property')

                if elem[0] in target_fields:
                    property.attrib['lookup'] = "true"
                else:
                    property.attrib['lookup'] = "false"

                name = SubElement(property, 'name')
                name.text = elem[0]

                comparator = SubElement(property, 'comparator')
                comparator.text = elem[2]

                low = SubElement(property, 'low')
                low.text = elem[3]

                high = SubElement(property, 'high')
                high.text = elem[4]

                schema.append(property)

        #файл
        csv = etree.Element('csv')
        param = SubElement(csv, 'param')
        param.attrib['name'] = 'input-file'
        param.attrib['value'] = csv_database

        #разделитель
        param = SubElement(csv, 'param')
        param.attrib['name'] = 'separator'
        param.attrib['value'] = ','

        for elem in fields:
            if elem[0] == "id":
                column = SubElement(csv, 'column')
                column.attrib['name'] = elem[0]
                column.attrib['property'] = elem[0]
            else:
                column = SubElement(csv, 'column')
                column.attrib['name'] = elem[0]
                column.attrib['property'] = elem[0]
                column.attrib['cleaner'] = elem[1]

        root.append(csv)


        s = etree.tostring(root, pretty_print=True)
        with open("%s/xmlconfig.xml" % dirpath, "w") as xmlfile:
            xmlfile.write(s)

        print s
        print("\n")


    def duke(self, dirpath):

        xmlpath = dirpath+"/xmlconfig.xml"
        duke_project_path = "/home/smith/duke-1.2"
        command = ['java', '-cp', duke_project_path + '/duke-1.2.jar:' + duke_project_path + '/lucene-core-4.0.0.jar:' +
                      duke_project_path + '/lucene-analyzers-common-4.0.0.jar',
                  'no.priv.garshol.duke.Duke',
                  "--showmatches", xmlpath]

        with open(dirpath + "/out_file.txt", 'w') as f:
            subprocess.call(command, stdout=f)


    def parse_output(self, file):
        merge = []
        with open(file, 'r') as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if line[0:5] == 'MATCH':

                    line1 = lines[i+1].split()
                    line2 = lines[i+2].split()

                    merge.append([line1[1][1:len(line1[1])-2], line2[1][1:len(line2[1])-2], line[6:9]])

            merge = sorted(merge, key=lambda mass: mass[2], reverse=True)
        return merge

    def to_mongo(self, merge, fields, doc_type, dirpath):

        docs = []
        for elem in merge:
            doc = {}

            doc["ids"] = [elem[0], elem[1]]
            doc["rate"] = elem[2]


            def doc_convert(doc_id):
                doc = ConnectionManager.elastic_raw.get(ConnectionManager.elastic_raw.default_indices[0], "doc", doc_id)
                str_doc = json.dumps(doc)
                dict_doc = json.loads(str_doc)
                return dict_doc

            dict_doc1 = doc_convert(elem[0])
            dict_doc2 = doc_convert(elem[1])

            facets = {}
            for field in fields[1:]:

                if data_utils.dict_get(dict_doc1, field) not in [None, []] and data_utils.dict_get(dict_doc2, field) not in [None, []]:
                    facets[str(field).replace(".", "/")] = [data_utils.dict_get(dict_doc1, field), data_utils.dict_get(dict_doc2, field)]
                elif data_utils.dict_get(dict_doc1, field) not in [None, []] and data_utils.dict_get(dict_doc2, field) in [None, []]:
                    facets[str(field).replace(".", "/")] = data_utils.dict_get(dict_doc1, field)
                elif data_utils.dict_get(dict_doc2, field) not in [None, []] and data_utils.dict_get(dict_doc1, field) in [None, []]:
                    facets[str(field).replace(".", "/")] = data_utils.dict_get(dict_doc2, field)

            doc["facets"] = facets

            docs.append(doc)


        for doc in docs:
            print doc
        print("\n")

        ConnectionManager.mongodb.duplicates.save({"collection": docs, "datetime": datetime.datetime.now(), "doc_type": doc_type})

        print("Collection was added to MongoDB")
        print("\n")
        shutil.rmtree(dirpath)
        print ("Temporary dir '%s' is deleted" % dirpath)
        print("\n")



