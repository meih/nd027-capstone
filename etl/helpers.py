import configparser

conf = configparser.ConfigParser()
conf.read("config.ini")

input_dir = conf['DIRECTORY']['INPUT']
output_dir = conf['DIRECTORY']['OUTPUT']

# data files (input)
station_address_file = input_dir + conf['DATA']['STATION_ADDRESS']
station_metro_file = input_dir + conf['DATA']['STATION_METRO']
station_toei_file = input_dir + conf['DATA']['STATION_TOEI']
timetable_metro_file = input_dir + conf['DATA']['TIMETABLE_METRO']
timetable_toei_file = input_dir + conf['DATA']['TIMETABLE_TOEI']
survey_metro_file = input_dir + conf['DATA']['SURVEY_METRO']
survey_toei_file = input_dir + conf['DATA']['SURVEY_TOEI']
population_file = input_dir + conf['DATA']['POPULATION']
zip_code_file = input_dir + conf['DATA']['ZIP_CODE']

# parquet files (output)
stations_parquet = output_dir + 'stations'
timetables_parquet = output_dir + 'timetables'
population_parquet = output_dir + 'stations'
passengers_parquet = output_dir + 'passengers'

# data check conditions
checks = [
    {
        'test_sql': "SELECT COUNT(*) AS num FROM stations",
        'expected_result': 0,
        'comparison': '>',
        'target': 'num',
        'table': 'stations',
        'parquet': stations_parquet
    },
    {
        'test_sql': "SELECT COUNT(*) AS num FROM timetables",
        'expected_result': 0,
        'comparison': '>',
        'target': 'num',
        'table': 'timetables',
        'parquet': timetables_parquet
    },
    {
        'test_sql': "SELECT COUNT(*) AS num FROM population",
        'expected_result': 0,
        'comparison': '>',
        'target': 'num',
        'table': 'population',
        'parquet': population_parquet
    },
    {
        'test_sql': "SELECT COUNT(*) AS num FROM passengers",
        'expected_result': 0,
        'comparison': '>',
        'target': 'num',
        'table': 'passengers',
        'parquet': passengers_parquet
    },
]
