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


class FindDuplicatesDukeJob(BaseJob):

    def check_job(self, job):
        return job.get("action") == "find_duplicates_duke"

    def process_job(self, job):

        documents = ConnectionManager.elastic_raw.search(query=job.get("query"))
        doc_type = documents[0].doctypes[0]
        fields = json.loads(job.get("fields")).keys()
        print("\n")
        print("Our fields:")
        for field in fields:
            print field


        #создаем временную директорию
        dirpath = mkdtemp()
        print ("Temporary dir '%s' is created" % dirpath)

        self.make_csv(fields, dirpath, documents)

        #формируем вектор для конфигурационного файла
        for i, field in enumerate(fields):
            tmp = []
            tmp.append(field)
            tmp.append("no.priv.garshol.duke.cleaners.LowerCaseNormalizeCleaner")
            tmp.append("no.priv.garshol.duke.comparators.Levenshtein")
            tmp.append("0.1")
            tmp.append("0.7")
            fields[i] = tmp

        #опорные поля, по которым будут формироваться кандидаты для сравнения
        if doc_type == "person":
            target_fields = ["facets.person.Name",
                          "facets.person.birthday",
                          "facets.person.personal.reg_address_str",
                          "facets.person.personal.region"]


        xml = self.xml_config(fields, target_fields, dirpath, dirpath+"/database_csv.csv")
        self.duke(dirpath)
        self.parse_output(dirpath+"/out_file.txt", dirpath)


    def make_csv(self, fields, dirpath, documents):
        #открываем на запись базу
        with open("%s/database_csv.csv" % dirpath, "w") as database:
            writer = csv.writer(database)
            fields.insert(0, "id")
            writer.writerow(fields)

            all_fields = TermManagerClient.get_json("provider/fields")

            #пишем сами объекты
            for document in documents:
                row = []
                row.append(document.docid)
                for field in fields[1:]:
                    if all_fields[field]['datatype'] == "string":
                        try:
                            #print "row.append(document." + field + ")"
                            exec("row.append(document." + field + ")")
                        except AttributeError:
                            row.append(None)
                        except SyntaxError:
                            row.append(None)
                    else:
                        row.append(None)
                writer.writerow(row)



    #генерация конфигурационного файла XML
    def xml_config(self, fields, target_fields, dirpath, csv_database):

        root = etree.Element('duke')
        schema = SubElement(root, 'schema')

        threshold = etree.Element('threshold')
        threshold.text = "0.5"
        schema.append(threshold)

        maybethreshold = etree.Element('maybe-threshold')
        maybethreshold.text = "0.3"
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


    def duke(self, dirpath):

        xmlpath = dirpath+"/xmlconfig.xml"
        duke_project_path = "/home/smith/duke-1.2"
        command = ['java', '-cp', duke_project_path + '/duke-1.2.jar:' + duke_project_path + '/lucene-core-4.0.0.jar:' +
                      duke_project_path + '/lucene-analyzers-common-4.0.0.jar',
                  'no.priv.garshol.duke.Duke',
                  "--showmatches", xmlpath]

        with open(dirpath + "/out_file.txt", 'w') as f:
            subprocess.call(command, stdout=f)


    def parse_output(self, file ,dirpath):
        merge = []
        with open(file, 'r') as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if line[0:5] == 'MATCH':

                    line1 = lines[i+1].split()
                    line2 = lines[i+2].split()

                    merge.append([line1[1][1:len(line1[1])-2], line2[1][1:len(line2[1])-2], line[6:9]])

            merge = sorted(merge, key=lambda mass: mass[2], reverse=True)

        for elem in merge:
            print elem

        shutil.rmtree(dirpath)
