import pandas as pd


dcipher_clms = [           
                     'reporting_jurisdiction',
                     'county_names',
                     'other_jurisdiction',
                     'zipcode',
                     'population_served',
                     'sewage_travel_time',
                     'sample_location',
                     'sample_location_specify',
                     'institution_type',
                     'epaid',
                     'wwtp_name',
                     'wwtp_jurisdiction',
                     'capacity_mgd',
                     'industrial_input',
                     'stormwater_input',
                     'influent_equilibrated',
                     'sample_type',
                     'composite_freq',
                     'sample_matrix',
                     'collection_storage_time',
                     'collection_storage_temp',
                     'pretreatment',
                     'pretreatment_specify',
                     'solids_separation',
                     'concentration_method',
                     'extraction_method',
                     'pre_conc_storage_time',
                     'pre_conc_storage_temp',
                     'pre_ext_storage_time',
                     'pre_ext_storage_temp',
                     'tot_conc_vol',
                     'ext_blank',
                     'rec_eff_target_name',
                     'rec_eff_spike_matrix',
                     'rec_eff_spike_conc',
                     'pasteurized',
                     'pcr_target',
                     'pcr_target_ref',
                     'pcr_type',
                     'lod_ref',
                     'hum_frac_target_mic',
                     'hum_frac_target_mic_ref',
                     'hum_frac_target_chem',
                     'hum_frac_target_chem_ref',
                     'other_norm_name',
                     'other_norm_ref',
                     'quant_stan_type',
                     'stan_ref',
                     'inhibition_method',
                     'num_no_target_control',
                     'sample_collect_date',
                     'sample_collect_time',
                     'time_zone',
                     'flow_rate',
                     'ph',
                     'conductivity',
                     'tss',
                     'collection_water_temp',
                     'equiv_sewage_amt',
                     'sample_id',
                     'lab_id',
                     'test_result_date',
                     'sars_cov2_units',
                     'sars_cov2_avg_conc',
                     'sars_cov2_std_error',
                     'sars_cov2_cl_95_lo',
                     'sars_cov2_cl_95_up',
                     'sars_cov2_below_lod',
                     'lod_sewage',
                     'ntc_amplify',
                     'rec_eff_percent',
                     'inhibition_detect',
                     'inhibition_adjust',
                     'hum_frac_mic_conc',
                     'hum_frac_mic_unit',
                     'hum_frac_chem_conc',
                     'hum_frac_chem_unit',
                     'other_norm_conc',
                     'other_norm_unit',
                     'quality_flag']

county_keys = {    
                "ada":"Adams",
                "aso":"Asotin",
                "ben":"Benton",
                "che":"Chelan",
                "clm":"Clallam",
                "clk":"Clark",
                "col":"Columbia",
                "cow":"Cowlitz",
                "dou":"Douglas",
                "fer":"Ferry",
                "fra":"Franklin",
                "gar":"Garfield",
                "grt":"Grant",
                "ghb":"Grays Harbor",
                "isl":"Island",
                "jef":"Jefferson",
                "kin":"King",
                "ksp":"Kitsap",
                "ktt":"Kittitas",
                "klk":"Klickitat",
                "lew":"Lewis",
                "lin":"Lincoln",
                "mas":"Mason",
                "oka":"Okanogan",
                "pac":"Pacific",
                "por":"Pend Oreille",
                "per":"Pierce",
                "san":"San Juan",
                "skg":"Skagit",
                "skm":"Skamania",
                "sno":"Snohomish",
                "spo":"Spokane",
                "ste":"Stevens",
                "thu":"Thurston",
                "wah":"Wahkiakum",
                "wal":"Walla Walla",
                "wha":"Whatcom",
                "wit":"Whitman",
                "yak":"Yakima",
                }


def condense_county_columns(df_pid170):
    """
    REDCap PID170 contains a unique column for every possible county (values 0 or 1) yes or no interprotation of said county
    
    Convert all the county columns into a single column containing a list of full county name values
    """
    
    df_pid170 = df_pid170.copy()

    county_columns = df_pid170.filter(regex="county_names").columns.copy()#identify county name columns

    df_counties = df_pid170[county_columns].copy()


    #Convert data frame to dictionary in format {index: [abbreviated county names]}
    raw_county_names = {}

    for row_index in df_counties.index:

        row_series = df_counties.loc[row_index,:]

        a = list(row_series[row_series == 1].index)
        a = [i[-3:] for i in a]

        raw_county_names[row_index] = a 


    #Convert dictionary of raw county names into full names based on county key:value pairs. 
    full_county_names = {}

    for key, value in raw_county_names.items():
        full_county_names[key] = [county_keys[i] for i in value]


    df_pid170 = df_pid170.loc[:, ~df_pid170.columns.isin(county_columns)].copy()
    df_pid170.loc[:,["county_names"]] = pd.Series(full_county_names)
    
    return df_pid170


def wide_to_long(df_pid171):
    """
    REDCap PID171 is in wide format with unique sample ID's and PCR_target of n1 and n2 containing fields sars_cov2_below_lod (n1 and n2) and sars_cov2_avg_conc (n1 and n2). 
    
    Transform long format: repeat sample ID's for n1 target and n2 target (PCR_target field). Single column of sars_cov2_below_lod and sars_cov2_avg_conc. 
    
    """
    df_pid171 = df_pid171.reset_index().copy()
    
    #identify columns to melt, and all the rest
    melt_clms = ['n1_sars_cov2_avg_conc', 'n2_sars_cov2_avg_conc', 'n1_sars_cov2_below_lod', 'n2_sars_cov2_below_lod']
    not_melt_clms = df_pid171.columns[~df_pid171.columns.isin(melt_clms)]
    
    #perform melt for avg_conc and keep all other columns
    df_melt_conc = pd.melt(df_pid171, value_vars = ['n1_sars_cov2_avg_conc', 'n2_sars_cov2_avg_conc'], var_name = "pcr_target", value_name = 'sars_cov2_avg_conc', id_vars = not_melt_clms )
    #perform melt for below_lod and only keep the value column (below_lod)
    df_melt_lod = pd.melt(df_pid171, value_vars = ['n1_sars_cov2_below_lod', 'n2_sars_cov2_below_lod'], var_name = "pcr_target", value_name = 'sars_cov2_below_lod', id_vars = ["sample_id"] )
    
    #change the PCR_target column to only first 2 letters (n1 or n2)
    df_melt_lod["pcr_target"] = df_melt_lod["pcr_target"].str[0:2]
    df_melt_conc["pcr_target"] = df_melt_lod["pcr_target"].str[0:2]
    
    #merge the dataframes together
    df_pid171 = pd.merge(df_melt_conc, df_melt_lod, how = "inner", left_on = ["sample_id", "pcr_target"], right_on = ["sample_id", "pcr_target"])
    
    return df_pid171


def clean_merge(df_pid170, df_pid171, df_pid176):
    """
    merge REDCap dataframes into a final dataframe for DCIPHER. 
    
    """
    df_pid170 = df_pid170.copy()
    df_pid171 = df_pid171.copy()    
    df_pid176 = df_pid176.copy()
    
    #merge PID170 unto pid171 on sample_site_id
    merge_1 = df_pid171.merge(df_pid170, left_on = "sample_site_id", right_index = True)
    
    #final merge: PID176 unto the first merge
    not_needed = df_pid176.columns[~df_pid176.columns.isin(["zipcode", "pcr_target"])] #list of not needed columns (would be duplicates after next merge) in pid176
    complete = merge_1.merge(df_pid176.loc[:,not_needed], left_on="micro_lab_id", right_index=True) #final merge (excluding not needed columns)
    
    #select only final DCIPHER columns and minor modifications
    complete = complete.rename(columns = {"micro_lab_id":"lab_id"})
    complete = complete.loc[:,dcipher_clms]
    complete = complete.sort_values(by = ["sample_id"], ignore_index=True)
    
    return complete

def pid170_values_transform(df_pid170):
    """
    transform values in select fields from REDCap PID170
    """

    df_pid170 = df_pid170.copy()
    
    inst_keys = {
             1: 'not institution specific',
             2: 'correctional',
             3: 'long term care - nursing home',
             4: 'long term care - assisted living',
             5: 'other long term care',
             6: 'short stay acute care hospital',
             7: 'long term acute care hospital',
             8: 'child day care',
             9: 'k12',
             10: 'higher ed dorm',
             11: 'higher ed other',
             12: 'social services shelter',
             13: 'other residential building',
             14: 'ship',
             15: 'airplane',
             99: 'other, please specify'
                }
    
    storm_input = {
            1:"yes",
            0:"no"
                }
    
    sample_matrix = {
            1: 'raw wastewater',
            2: 'post grit removal',
            3: 'primary sludge',
            4: 'primary effluent',
            5: 'secondary sludge',
            6: 'secondary effluent',
            7: 'septage',
            8: 'holding tank'
                }
    
    sample_type = {
            '24F': '24-hr flow-weighted composite',
            '12F': '12-hr flow-weighted composite',
            '8F': '8-hr flow-weighted composite',
            '6F': '6-hr flow-weighted composite',
            '3F': '3-hr flow-weighted composite',
            '24T': '24-hr time-weighted composite',
            '12T': '12-hr time-weighted composite',
            '8T': '8-hr time-weighted composite',
            '6T': '6-hr time-weighted composite',
            '3T': '3-hr time-weighted composite',
            '24M': '24-hr manual composite',
            '12M': '12-hr manual composite',
            '8M': '8-hr manual composite',
            '6M': '6-hr manual composite',
            '3M': '3-hr manual composite',
            'G': 'grab'
                }
    
    # Changing REDCap key:values for institution_type
    df_pid170['institution_type'] = df_pid170['institution_type'].map(inst_keys)
    
    # Changing REDCap key:values for stormwater_input
    df_pid170['stormwater_input'] = df_pid170['stormwater_input'].map(storm_input)
    
    # Changing REDCap key:values for 'influent_equilibrated'
    df_pid170['influent_equilibrated'] = df_pid170['influent_equilibrated'].map(storm_input)
    
    # Changing REDCap key:values for 'sample_matrix'
    df_pid170["sample_matrix"] = df_pid170["sample_matrix"].map(sample_matrix)
    
    # Changing REDCap key:values for 'sample_type'
    df_pid170["sample_type"] = df_pid170["sample_type"].map(sample_type)

    return df_pid170
