import cx_Oracle
# cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_7")
import camelot
# %matplotlib notebook
import json
import re
import numpy as np
import pandas as pd

class edms_file_prep:
    def __init__(self, planet_names, star_names=None):
        self.planet_names = planet_names
        if not star_names:
            self.star_names = [name[:-2] for name in planet_names] # assume planet name is star name + " b"
        else:
            self.star_names = star_names
         
        self.new_stars = []
        self.new_planets = []
        self.star_ids = []
        self.planet_ids = []
            
        dsnStr = cx_Oracle.makedsn("nexscidb4", "1565", sid="exotest1")
        connection = cx_Oracle.connect(user='exoprep', password='pr3p_6402', dsn=dsnStr)
        self.cur = connection.cursor()
        
        for i in range(len(self.planet_names)):
            planet_name = self.planet_names[i]
            star_name = self.star_names[i]
            
            # will this work if the planet name is different than the star name + b/.01
            # would have to use sa_convert path
            self.cur.execute(f"SELECT * FROM names WHERE display_name like '{star_name}'")
            query_results = [row for row in self.cur]
            
            if len(query_results) == 0:
                self.new_stars.append(True)
                self.new_planets.append(True)
                
                self.star_ids.append(False)
                self.planet_ids.append(False)
                
            else:
                self.new_stars.append(False)
                
                star_id = False
                planet_id = False

                for result in query_results:
                    id_ = result[0]
                    if id_[0] == "2":
                        if result[2] == star_name: # if the star only goes by a different alias this wouldn't catch it
                            star_id = id_
                        else:
                            continue
#                     elif id_[0] == "3":
#                         if (result[2][-1] == planet_name[-1]) or (planet_name[-1] == "b" and result[2][-3:] == ".01") or (planet_name[-1] == "c" and result[2][-3:] == ".02") or (planet_name[-1] == "d" and result[2][-3:] == ".03"):
#                             planet_id = id_
#                         else:
#                             continue
                 
                self.star_ids.append(star_id)
    
                self.cur.execute(f"SELECT * FROM sa_convert where ST_ID like '{star_id}'")
                query_results = [row for row in self.cur]
                
                new_planet = True
                for result in query_results:
                    potential_planet_id = result[5]
                    if potential_planet_id == None:
#                         self.planet_ids.append(None)
                        continue
                        
                    planet_display_names = []
                    self.cur.execute(f"SELECT * FROM names WHERE OBJECTID like '{potential_planet_id}'")
                    for row in self.cur:
                        if (row[2][-1] == planet_name[-1]) or (planet_name[-1] == "b" and row[2][-3:] == ".01") or (planet_name[-1] == "c" and row[2][-3:] == ".02") or (planet_name[-1] == "d" and row[2][-3:] == ".03"):
                            planet_id = potential_planet_id
                            break

                if potential_planet_id == None:
                    planet_id = False
                            
                
                self.planet_ids.append(planet_id)
                                
                if planet_id:
                    self.new_planets.append(False)
                else:
                    self.new_planets.append(True)
                
#             print("# star IDs:", self.star_names[i], self.star_ids[i])
#             print("# planet IDs:", self.planet_names[i], self.planet_ids[i])
#             print()
                    

        
    def load_datum_mappings(self):
        with open(f'..\\..\\..\\automation\\stellar_datum_map.json', encoding="utf8") as jsonfile:
            param_mapping = json.load(jsonfile)
            self.stellar_mapping_keys = list(param_mapping.keys())
            self.stellar_mapping_values = list(param_mapping.values())

        with open(f'..\\..\\..\\automation\\planetary_datum_map.json', encoding="utf8") as jsonfile:
            param_mapping = json.load(jsonfile)
            self.planetary_mapping_keys = list(param_mapping.keys())
            self.planetary_mapping_values = list(param_mapping.values())

        with open(f'..\\..\\..\\automation\\microlensing_datum_map.json', encoding="utf8") as jsonfile:
            param_mapping = json.load(jsonfile)
            self.micro_mapping_keys = list(param_mapping.keys())
            self.micro_mapping_values = list(param_mapping.values())
    
    def header(self, bibcode, preparer="nick"):
        print(f"PREPARER: {preparer}")
        print("DESCRIPTION: EDMS Production File")
        print("FILETYPE: EDMS")
        print()
        print("#")
        print("# Reference")
        print(f"ECMD | REF | reference | -1 | add | mode ads | bibcode {bibcode} |")
        print()
        
    def solutions(self, n_solutions_per_planet=[1]):
        self.n_solutions_per_planet = n_solutions_per_planet
        self.planet_solutions = [[] for x in range(len(np.unique(self.star_names)))]
        print("#")
        print("# Solutions")
        sol_strings = []
        if n_solutions_per_planet == [1]:
            sol_strings.append(f"ECMD | SOL | reference | -1 | add | soln 1 | type Published Confirmed")
        else:
            for i in range(len(np.unique(self.star_names))):
                for j in range(self.n_solutions_per_planet[i]):
                    sol_strings.append(f"ECMD | SOL | reference | -1 | add | soln {i+1}{j+1} | type Published Confirmed")
                    self.planet_solutions[i].append(f"{i+1}{j+1}")
        
        equalized_strings = self.equalize_string_length(sol_strings)    
        for i in equalized_strings:
            print(i)

        print()
                  
    def aliases(self):
        print("#")
        print("# Aliases")

        for i in range(len(self.planet_names)):
            planet_name = self.planet_names[i]
            star_name = self.star_names[i]
            
            # would not work if there exists a planet without a star
            if self.new_stars[i]:
                
                if "-" in star_name and " " in star_name:
                    catalog = star_name.split("-")[0]
                    catalog = catalog.split(" ")[0]
                elif "+" in star_name and " " in star_name:
                    catalog = star_name.split("+")[0]
                    catalog = catalog.split(" ")[0]
                elif "-" in star_name:
                    catalog = star_name.split("-")[0]
                elif "+" in star_name:
                    catalog = star_name.split("+")[0]
                else:
                    catalog = star_name.split(" ")[0] 

                # print("# no existing stellar aliases")
                
                alias_strings = []
                alias_strings.append(f"ECMD | ALS | system | -2{i+1} | add_def | alias {star_name}   | catalog {catalog}")
                alias_strings.append(f"ECMD | ALS | star   | -3{i+1} | add_def | alias {star_name}   | catalog {catalog}")
                alias_strings.append(f"ECMD | ALS | planet | -4{i+1} | add_def | alias {planet_name} | catalog {catalog}")
                
                equalized_strings = self.equalize_string_length(alias_strings)    
                for i in equalized_strings:
                    print(i)

                print()
            
            else:
                
                self.cur.execute(f"SELECT * FROM names WHERE OBJECTID like '{self.star_ids[i]}'")
                star_display_names = [row[2] for row in self.cur]

                # print("# existing star display names:", star_display_names)

                planet_display_names = []

                if not self.new_planets[i]:

                    self.cur.execute(f"SELECT * FROM names WHERE OBJECTID like '{self.planet_ids[i]}'")
                    for row in self.cur:
                        planet_display_names.append(row[2])
                    pl_obj_id = planet_display_names[0]

                else:
                    pl_obj_id = f"-3{i}"

                # print("# existing planet display names:", planet_display_names)
#                 print(planet_name)
                alias_strings = []
                for alias in star_display_names:
                    if alias not in [p[:-2] for p in planet_display_names]:
                        new_planet_alias = alias + planet_name[-2:]

                        if "-" in alias and " " in alias:
                            catalog = alias.split("-")[0]
                            catalog = catalog.split(" ")[0]
                        elif "+" in alias and " " in alias:
                            catalog = alias.split("+")[0]
                            catalog = catalog.split(" ")[0]
                        elif "-" in alias:
                            catalog = alias.split("-")[0]
                        elif "+" in alias:
                            catalog = alias.split("+")[0]
                        else:
                            catalog = alias.split(" ")[0] 
                            
                        if new_planet_alias.split(" ")[0] == star_name.split(" ")[0]:
                            alias_strings.append(f"ECMD | ALS | planet | {pl_obj_id} | add_def | alias {new_planet_alias} | catalog {catalog}")
                        else:
                            alias_strings.append(f"ECMD | ALS | planet | {pl_obj_id} | add | alias {new_planet_alias} | catalog {catalog}")

                equalized_strings = self.equalize_string_length(alias_strings)    
                for i in equalized_strings:
                    print(i)
                            
                print()
    
    def orb_configs(self):
        print("#")
        print("# Orbital Configurations")
        orb_strings = []
        for i in range(len(self.planet_names)):

            self.cur.execute(f"SELECT * FROM sa_convert where ST_ID like '{self.star_ids[i]}'") # if star id is false result will be empty
            query_results = [row for row in self.cur]
            
            if len(query_results) == 0:
                # subsequent orbs should use update, so second new planet should use update and not add
                orb_strings.append(f"ECMD | ORB | system | {self.star_names[i]} | add | star {self.star_names[i]} | planet {self.planet_names[i]}")
            else:
                orb_exists = False
                for result in query_results:
                    if result[5] == self.planet_ids[i]:
                        orb_exists = True
                        
                if not orb_exists:
                    orb_strings.append(f"ECMD | ORB | system | {self.star_names[i]} | update | star {self.star_names[i]} | planet {self.planet_names[i]}")
                else:
                    pass
        
        equalized_strings = self.equalize_string_length(orb_strings)    
        for i in equalized_strings:
            print(i)      
                
        print()
                  
    def discoveries(self, years, methods, facs, teles=None, instrs=None, locs=None):
        discovery_facilities = pd.read_csv("..\\..\\..\\automation\\discovery_facilities.txt", header=0)

        print("#")
        print("# Discoveries")
        disc_strings = []
        for i in range(len(self.planet_names)):
            discovery_info = discovery_facilities[discovery_facilities["FACILITY"] == facs[i]]
            if facs[i] == "Multiple Facilities":
                tele = "Multiple Telescopes"
                instr = "Multiple Instruments"
                loc = "Multiple Locales"
            elif (facs[i] is not None) and (teles[i] is None) and (instrs[i] is None) and (locs[i] is None):                
                tele = discovery_info["TELESCOPE"].values[0].strip()
                instr = discovery_info["INSTRUMENT"].values[0].strip()
                loc = discovery_info["LOCALE"].values[0].strip()
            else:
                tele = teles[i]
                instr = instrs[i]
                loc = locs[i]
            
            self.cur.execute(f"SELECT * FROM DISCOVERIES WHERE OBJECTID like  '{self.planet_ids[i]}'") # if star id is false result will be empty
            query_results = [row for row in self.cur]
            
            if len(query_results) == 0:
                disc_strings.append(f"ECMD | DSC | planet | {self.planet_names[i]} | add | refid -1 | method {methods[i]} | year {years[i]} | facility {facs[i]} | telescope {tele} | instrument {instr} | locale {loc}")
            else:
                # if it's a candidate (eg TOI) just update discovery year (check disposition)
                self.cur.execute(f"SELECT * FROM DISPOSITIONS WHERE OBJECTID like '{self.planet_ids[i]}'")
                query_results = [row for row in self.cur]
                
                confirmed_exists = False
                for result in query_results:
                    if result[2] == "CONFIRMED":
                        confirmed_exists = True
                        
                if not confirmed_exists:
                    disc_strings.append(f"ECMD | DSC | planet | {self.planet_names[i]} | update | refid -1 | year {years[i]}")
                else:
                    pass

        equalized_strings = self.equalize_string_length(disc_strings)    
        for i in equalized_strings:
            print(i)
            
        print()       
                                    
    def detections(self, methods): # methods could have a different length than planet_names (multiple methods per planet name)
        print("#")
        print("# Detections")
        
        det_strings = []
        for i in range(len(self.planet_names)):
            # new methods are added regardless of what already exists (multiple identical methods can exist); assume each planet gets each method
            # is update never used here?
            for method in methods:
                det_strings.append(f"ECMD | DET | planet | {self.planet_names[i]} | add | refid -1 | method {method}")
      
        equalized_strings = self.equalize_string_length(det_strings)    
        for i in equalized_strings:
            print(i)
    
        print()
                  
    def dispositions(self, years, disps):
        print("#")
        print("# Dispositions")
        disp_strings = []
        # are dispositions always added (never updated)?
        for i in range(len(self.planet_names)):
            self.cur.execute(f"SELECT * FROM DISPOSITIONS WHERE OBJECTID like '{self.planet_ids[i]}'")
            query_results = [row for row in self.cur]
            
            if len(query_results) == 0:
                disp_strings.append(f"ECMD | DSP | planet | {self.planet_names[i]} | add | refid -1 | disp {disps[i]} | year {years[i]} | controv 0 | archive 1")
            else:
                disp_exists = False
                for result in query_results:
                    if result[2] == disps[i]:
                        disp_exists = True
                        
                if not disp_exists:
                    disp_strings.append(f"ECMD | DSP | planet | {self.planet_names[i]} | add | refid -1 | disp {disps[i]} | year {years[i]} | controv 0 | archive 1")
                else:
                    pass
                
        equalized_strings = self.equalize_string_length(disp_strings)    
        for i in equalized_strings:
            print(i)
        print()                  
          
    def join(self, default_soln=1):
        print("#")
        print("# Join")
        join_strings = []
        for i in range(len(self.planet_names)):
            if self.n_solutions_per_planet == [1]:
                string = f"ECMD | PSJ | planet | {self.planet_names[i]} | add_def | plref -1 | plsol 1 | stref -1 | stsol 1"
                join_strings.append(string)
            else:
                for j in range(self.n_solutions_per_planet[i]):
                    soln_number = j + 1
                    if soln_number == default_soln: 
                        string = f"ECMD | PSJ | planet | {self.planet_names[i]} | add_def | plref -1 | plsol {i+1}{j+1} | stref -1 | stsol {i+1}{j+1}"
                    else:
                        string = f"ECMD | PSJ | planet | {self.planet_names[i]} | add     | plref -1 | plsol {i+1}{j+1} | stref -1 | stsol {i+1}{j+1}"

                    if self.planet_names[i][:3] == "KMT" or self.planet_names[i][:3] == "OGL":
                        string += f"| mlref -1 | mlsol {i+1}{j+1} | syref -1 | sysol {i+1}{j+1}"
                    
                    join_strings.append(string)
            
        equalized_strings = self.equalize_string_length(join_strings)    
        for i in equalized_strings:
            print(i)        
        print()

    def notes(self, type):
        print("#")
        print("# notes")
        notes_strings = []
        for i in range(len(self.planet_names)):
            if type == "micro":
                for j in range(self.n_solutions_per_planet[i]):
                    notes_strings.append(f"ECMD | NTE | planet | {self.planet_names[i]} | add | refid -1 | type ML_MODEL_DESCRIPTION | soln {i+1}{j+1} | note x")
            else:
                notes_strings.append(f"ECMD | NTE | planet | {self.planet_names[i]} | add | refid -1 | note x")

        equalized_strings = self.equalize_string_length(notes_strings)    
        for i in equalized_strings:
            print(i)        
        print()
        
    def write_string(self, object_type, object_name, soln, datum_name, data):
        if "±" in data:
            data = re.search("[\d\.-]+±[\d\.-]+", data.replace(" ", "")).group()
            data = data.split("±")
            data = [d.strip() for d in data]
            value = data[0]
            lower_error = upper_error = data[1]
                
            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value} | err1 {upper_error} | err2 -{lower_error} | lim 0"
        
        elif "(" in data and ")" in data:
            data = data.split("(")
            value = data[0]
            lower_error = upper_error = data[1].strip(")")
            n_decimals = len(value.split(".")[1])
            error_digits = len(lower_error)
            lower_error = upper_error = "0." + "0"*(n_decimals-error_digits) + lower_error
                
            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value} | err1 {upper_error} | err2 -{lower_error} | lim 0"
        
        elif ("+" in data) and ("-" in data):
            data = re.split('\+|\-', data)
            data = [d.strip() for d in data]
            value = data[0]
            upper_error = data[1]
            lower_error = data[2]

            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value} | err1 {upper_error} | err2 -{lower_error} | lim 0"
       
        elif "<" in data:
            value = re.sub(" *< *", "", data)
                
            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value} | lim 1"
        
        elif ">" in data:
            value = re.sub(" *> *", "", data)

                
            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value} | lim -1"
        
        else:
            value = data
            dtm_string = f"ECMD | DTM | {object_type} | {object_name} | add | refid -1 | soln {soln} | datum {datum_name} | value {value}"

        if datum_name == "MET":
            dtm_string += " | detail [Fe/H]"
        elif datum_name == "TRANMID":
            dtm_string += " | detail BJD-TDB"
        elif datum_name == "TSEPMIN":
            dtm_string += " | detail HJD"
        elif datum_name == "ORBTPER":
            dtm_string += " | detail JD"

        return dtm_string
    
    def insert (self, source_str, insert_str, pos):
        return source_str[:pos] + insert_str + source_str[pos:]

    # def convert_mag(self, string, current_mag, final_mag): # mag sign matters
    #     period_pos = string.index(".")
    #     new_string = string.replace(".", "")
    #     movement = 1*(current_mag-final_mag)
    #     new_string = self.insert(new_string, ".", period_pos+movement)
    #     try:
    #         re.search("$[1-9]+0*\.", new_string).group()
    #     except AttributeError:
    #         new_string = new_string.lstrip("0")

    #     if new_string[0] == ".":
    #         new_string = self.insert(new_string, "0", 0)
    #     elif new_string[-1] == ".":
    #         new_string = new_string[:-1]

    #     return new_string
    
    def equalize_string_length(self, string_list):
        component_lengths = [[] for i in range(13)]
        
        # ECMD | use case | object type | object id | action | refid | soln | datum | value | err1 | err2 | lim |
        # ECMD | use case | object type | object id | action | alias | catalog |
        
        for string in string_list:
            tokenized_string = string.split(" |")
            for i in range(len(tokenized_string)):
                component_lengths[i].append(len(tokenized_string[i]))
        
        new_strings = []
        for i in range(len(string_list)):
            split_string = string_list[i].split(" |")
            new_string = ""
            for j in range(len(split_string)):
                value = split_string[j]
                while len(value) != max(component_lengths[j]):
                    value += " "
                new_string += value + " |"

        
            new_strings.append(new_string)
            
        return new_strings

    def extract_datums(self, object_type, df, planet_index=0, label_col=0, value_col=1, soln=1, object_name=None):
        table = df.astype(str)

        self.load_datum_mappings()
        if object_type == "star":
            print("#")
            print("# stellar parameters")
            print("# from Table")
            mapping_keys = self.stellar_mapping_keys
            mapping_values = self.stellar_mapping_values
            if object_name == None:
                object_name = self.star_names[planet_index]
        elif object_type == "planet":
            print("#")
            print("# planetary parameters")
            print("# from Table")
            mapping_keys = self.planetary_mapping_keys
            mapping_values = self.planetary_mapping_values
            if object_name == None:
                object_name = self.planet_names[planet_index]
        elif object_type == "micro":
            print("#")
            print("# microlensing parameters")
            print("# from Table")
            mapping_keys = self.micro_mapping_keys
            mapping_values = self.micro_mapping_values
            if object_name == None:
                object_name = self.planet_names[planet_index]

        datum_strings = []
        for i in range(len(table.index)):
            parameter = str(table.iloc[i,label_col])

#             if "," in parameter and "(" not in parameter:
#                 parameter = parameter.split(",")
#                 parameter = [p.lower() for p in parameter]

            found_parameter = False
            for j in range(len(mapping_values)):
                if not found_parameter:
                    for name in mapping_values[j]:

                        if type(parameter) == list:
                            found_parameter = any([name.lower() == p for p in parameter])
                        else:
                            found_parameter = name.lower() == parameter.lower()

                        if found_parameter:
                            data = table.iloc[i,value_col]
                            if "–" in data:
                                data = re.sub(" *– *", "-", data)
                            if "−" in data:
                                data = re.sub(" *− *", "-", data)
                            if "×" in data:
                                data = re.sub(" *× *", "*", data)
                            if "(ﬁxed)" in data:
                                data = re.sub("\(ﬁxed\)", "", data)
                                              
                            if mapping_keys[j] == "MODELCHISQ" and "/" in data:
                                data = data.split("/")[0]

                            # if mapping_keys[j] == "TSEPMIN":
                            #     data = "245" + data
                            # if mapping_keys[j] == "ORBTPER":
                            #     data = "245" + data
            
                            if (mapping_keys[j] == "COLVIS" or mapping_keys[j] == "MAGIS" or mapping_keys[j] == "COLVISO" or mapping_keys[j] == "MAGISO") and ("(" in data and ")" in data):
                                data = data.replace('(','').replace(')','').split(",")
                                c_data = data[0].strip()
                                m_data = data[1].strip()

                                if mapping_keys[j] == "COLVIS" or mapping_keys[j] == "MAGIS":
                                    dtm_string = self.write_string(object_type, object_name, soln, "COLVIS", c_data)
                                    datum_strings.append(dtm_string)
                                    dtm_string = self.write_string(object_type, object_name, soln, "MAGIS", m_data)
                                    datum_strings.append(dtm_string)  
                                else:
                                    dtm_string = self.write_string(object_type, object_name, soln, "COLVISO", c_data)
                                    datum_strings.append(dtm_string)
                                    dtm_string = self.write_string(object_type, object_name, soln, "MAGISO", m_data)
                                    datum_strings.append(dtm_string)                                  

                            if soln == "all":
                                for soln_n in self.planet_solutions[planet_index]:
                                    dtm_string = self.write_string(object_type, object_name, soln_n, mapping_keys[j], data)
                                    datum_strings.append(dtm_string)
                            else:
                                dtm_string = self.write_string(object_type, object_name, soln, mapping_keys[j], data)
                                datum_strings.append(dtm_string)

                                
        equalized_strings = self.equalize_string_length(datum_strings)    
        for i in equalized_strings:
            print(i)
        
    def configure_datum_table(self, pdf_filename, flavor="stream", **kwargs):
        # pages="4", table_areas=['105,555,477,306'], row_tol=5, strip_text="\n", column_tol=5, layout_kwargs={'line_overlap': 0.3}
        pdf_path = f".\\papers\\{pdf_filename}.pdf"
        
        tables = camelot.read_pdf(filepath=pdf_path, flavor=flavor, **kwargs)
        camelot.plot(tables[0], kind='contour').show()
        table = tables[0].df
#         display(table)
        
        return table
    
            
    def convert_err(self, v, e1, e2, f):
        conv_v = f(v)
        conv_e1 = f(v+e1) - conv_v
        conv_e2 = conv_v - f(v-e2)
        return conv_v, conv_e1, conv_e2
    
    def count_sig_figs(self, number):
        # non-zeros are significant
        # leading zeros are not significant
        # zeros in between significant digits (across decimal point) are significant
        # trailing zeros to the right of the decimal are significant
        # trailing zeros to the left of the decimal are not significant

        number = re.sub("[^\d\.]", "", number)
        number = number.lstrip("0")

        if "." not in number:
            number = number.rstrip("0")

        else:
            number = number.replace(".", "")
            number = number.lstrip("0")

        return len(number)
        
    
    def convert_units(self, data, unit1, unit2):
        # convert unit1 -> unit2
        # could do this using astropy but i'll stick with the unit conversions page to be safe
        # https://confluence.ipac.caltech.edu/display/ExoplanetArchive/Unit+Conversions

        if "–" in data:
            data = re.sub(" *– *", "-", data)
        if "−" in data:
            data = re.sub(" *− *", "-", data)
        if "×" in data:
            data = re.sub(" *× *", "*", data)

        if "±" in data:
            data = re.search("[\d\.-]+±[\d\.-]+", data.replace(" ", "")).group()
            data = data.split("±")
            data = [d.strip() for d in data]
            v = data[0]
            e2 = e1 = data[1]
        elif "(" in data and ")" in data:
            data = data.split("(")
            v = data[0]
            e2 = e1 = data[1].strip(")")
            n_decimals = len(v.split(".")[1])
            error_digits = len(e2)
            e2 = e1 = "0." + "0"*(n_decimals-error_digits) + e2
        elif ("+" in data) and ("-" in data):
            data = re.split('\+|\-', data)
            data = [d.strip() for d in data]
            v = data[0]
            e1 = data[1]
            e2 = data[2]
        elif "<" in data:
            v = re.sub(" *< *", "", data)
            e1 = e2 = 0
        elif ">" in data:
            v = re.sub(" *> *", "", data)
            e1 = e2 = 0
        else:
            v = data
            e1 = e2 = 0

        skip_conv = False
        if unit1 == "years" and unit2 == "days":
            conv_factor = 365.25
        elif unit1 == "MJ" and unit2 == "MS":
            conv_factor = 0.000954594
        elif unit1 == "log" and unit2 == "lin":
            conv_v, conv_e1, conv_e2 = self.convert_err(float(v), float(e1), float(e2), np.exp)
            skip_conv = True
        elif unit1 == "log10" and unit2 == "lin":
            conv_v, conv_e1, conv_e2 = self.convert_err(float(v), float(e1), float(e2), lambda x: x**10)
            skip_conv = True
        elif unit1 == "kg" and unit2 == "g":
            conv_factor = 1000
        elif unit1 == "ppt" and unit2 == "percent": # ppt = parts per thousand
            conv_factor = 10**(-1)
        elif unit1 == "degrees" and unit2 == "radians":
            conv_factor = np.pi/180
        else:
            conv_factor = unit1/unit2 # eg 10**-2 -> 10**-3

        n_sig_figs_v = self.count_sig_figs(v)
        n_sig_figs_e1 = self.count_sig_figs(e1)
        n_sig_figs_e2 = self.count_sig_figs(e2)

        v = float(v)
        e1 = float(e1)
        e2 = float(e2)
        if not skip_conv:
            conv_v = conv_factor * v
            conv_e1 = conv_factor * (v+e1) - conv_v
            conv_e2 = conv_v - conv_factor * (v-e2)
        
        # https://stackoverflow.com/questions/3410976/how-to-round-a-number-to-significant-figures-in-python
        conv_v = '{:g}'.format(float('{:.{p}g}'.format(conv_v, p=n_sig_figs_v)))
        conv_e1 = '{:g}'.format(float('{:.{p}g}'.format(conv_e1, p=n_sig_figs_e1)))
        conv_e2 = '{:g}'.format(float('{:.{p}g}'.format(conv_e2, p=n_sig_figs_e2)))

        # assume trailing 0's could be missing
        n_sig_figs_conv_v = self.count_sig_figs(str(conv_v))
        # while n_sig_figs_conv_v < n_sig_figs_v:
        #     conv_v += "0"
        #     n_sig_figs_conv_v = self.count_sig_figs(str(conv_v))
        n_sig_figs_conv_e1 = self.count_sig_figs(str(conv_e1))
        # while n_sig_figs_conv_e1 < n_sig_figs_e1:
        #     conv_e1 += "0"
        #     n_sig_figs_conv_e1 = self.count_sig_figs(str(conv_e1))
        n_sig_figs_conv_e2 = self.count_sig_figs(str(conv_e2))
        # while n_sig_figs_conv_e2 < n_sig_figs_e2:
        #     conv_e2 += "0"
        #     n_sig_figs_conv_e2 = self.count_sig_figs(str(conv_e2))
        
        if conv_e1 == 0 and conv_e2 == 0:
            return f"{conv_v}"
        elif conv_e1 == conv_e2:
            return f"{conv_v}±{conv_e1}"
        else:
            return f"{conv_v}+{conv_e1}-{conv_e2}"

    def create_uniform_error(self, v1, v2):
        assert v1 < v2

        n_sig_figs_v1 = self.count_sig_figs(str(v1))
        n_sig_figs_v2 = self.count_sig_figs(str(v2))
        n_sig_figs = max(n_sig_figs_v1, n_sig_figs_v2)

        new_v = np.mean([v1, v2])
        new_e1 = v2 - new_v
        new_e2 = new_v - v1

        new_v = '{:g}'.format(float('{:.{p}g}'.format(new_v, p=n_sig_figs)))
        new_e1 = '{:g}'.format(float('{:.{p}g}'.format(new_e1, p=n_sig_figs)))
        new_e2 = '{:g}'.format(float('{:.{p}g}'.format(new_e2, p=n_sig_figs)))

        # ECMD | NTE | system | WISE J033605.05−014350.4   | add | refid -1 | Stellar age and mass and planetary mass estimated from rnges provided in ref. |

        return new_v, new_e1, new_e2