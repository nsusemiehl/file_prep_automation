import camelot
%matplotlib notebook

import pandas as pd
import numpy as np

class spectra_file_prep:
    def __init__(self, spec_type):

        self.spec_type = spec_type

    def header(self, spec_num, pl_name, bibcode, facility, instrument, note):
        print(f"\\SPEC_TYPE={spec_type}")
        print(f"\\SPEC_NUM={spec_num}")
        print(f"\\PL_NAME={pl_name}")
        print(f"\\BIBCODE={bibcode}")
        print(f"\\FACILITY={facility}")
        print(f"\\INSTRUMENT={instrument}")
        print(f"\\NOTE={note}")
        print("\\")
        if self.spec_type == "transmission":
            print("| CENTRALWAVELNG | BANDWIDTH | PL_RATROR | PL_RATRORERR1 | PL_RATRORERR2 | PL_RATRORLIM |PL_TRANDEP|PL_TRANDEPERR1|PL_TRANDEPERR2|PL_TRANDEPLIM|PL_RADJ|PL_RADJERR1|PL_RADJERR2|PL_RADJLIM|PL_TRANMID|PL_TRANMIDERR1|PL_TRANMIDERR2|PL_TRANMIDLIM|")
            print("|         double |    double |    double |        double |        double |          int |   double |       double |       double |         int | double|     double|     double|      int |   double |       double |       double |         int |")
            print("|         micron |    micron |           |               |               |              |  percent |              |              |             |       |           |           |          |          |              |              |             |")
            print("|           null |      null |      null |          null |          null |         null |     null |         null |         null |        null |  null |      null |      null |     null |     null |         null |         null |        null |")
        elif self.spec_type == "emission":
            print("|  CENTRALWAVELNG |  BANDWIDTH | ESPECLIPDEP | ESPECLIPDEPERR1 | ESPECLIPDEPERR2 | ESPECLIPDEPLIM |ESPBRITEMP|ESPBRITEMPERR1|ESPBRITEMPERR2|ESPBRITEMPLIM|")
            print("|         double  |    double  |    double   |        double   |        double   |          int   |   double |       double |       double |         int |")
            print("|         micron  |    micron  |             |       percent   |       percent   |      percent   |          |              |              |             |")
            print("|           null  |      null  |      null   |          null   |          null   |         null |     null |         null |         null |        null |")        
        
    def configure_datum_table(self, pdf_filename, flavor="stream", **kwargs):
    #     pdf_path = f".\\papers\\{pdf_filename}.pdf"
        pdf_path = f".\\papers\\{pdf_filename}.pdf"

        tables = camelot.read_pdf(filepath=pdf_path, flavor=flavor, row_tol=5, strip_text="\n", column_tol=5, **kwargs)
        camelot.plot(tables[0], kind='contour').show()
        table = tables[0].df

        return table