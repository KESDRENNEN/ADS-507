

from extract_bls import main_bls_pull
from extract_climate import extract_climate_data
from extract_fema import fema_combo

def main () :
    #main_bls_pull()
    #fema_combo()
    extract_climate_data()


if __name__ == "__main__":
    main()