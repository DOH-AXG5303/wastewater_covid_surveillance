import pandas as pd
import numpy as np

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
            'ada': 53001,
            'aso': 53003,
            'ben': 53005,
            'che': 53007,
            'clm': 53009,
            'clk': 53011,
            'col': 53013,
            'cow': 53015,
            'dou': 53017,
            'fer': 53019,
            'fra': 53021,
            'gar': 53023,
            'grt': 53025,
            'ghb': 53027,
            'isl': 53029,
            'jef': 53031,
            'kin': 53033,
            'ksp': 53035,
            'ktt': 53037,
            'klk': 53039,
            'lew': 53041,
            'lin': 53043,
            'mas': 53045,
            'oka': 53047,
            'pac': 53049,
            'por': 53051,
            'per': 53053,
            'san': 53055,
            'skg': 53057,
            'skm': 53059,
            'sno': 53061,
            'spo': 53063,
            'ste': 53065,
            'thu': 53067,
            'wah': 53069,
            'wal': 53071,
            'wha': 53073,
            'wit': 53075,
            'yak': 53077}


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
        full_county_names[key] = str([county_keys[i] for i in value])[1:-1] #convert list to string, drop brackets


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

def pid171_transform(df_pid171):
    """
    transform values in select fields from REDCap PID171
    """

    df_pid171 = df_pid171.copy()
    
    pretreatment = {
            1:"yes",
            0:"no"
            }
    
    units = {
            1: "copies/L wastewater",
            2: "log10 copies/L wastewater",
            3: "copies/g wet sludge",
            4: "log10 copies/g wet sludge",
            5: "copies/g dry sludge",
            6: "log10 copies/g dry sludge"
            }

    hum_frac_mic_unit = {
             1: 'copies/L wastewater',
             2: 'log10 copies/L wastewater',
             3: 'copies/g wet sludge',
             4: 'log10 copies/g wet sludge',
             5: 'copies/g dry sludge',
             6: 'log10 copies/g dry sludge'
            }

    hum_frac_chem_unit = {
             1: 'copies/L wastewater',
             2: 'log10 copies/L wastewater',
             3: 'copies/g wet sludge',
             4: 'log10 copies/g wet sludge',
             5: 'copies/g dry sludge',
             6: 'log10 copies/g dry sludge',
             7: 'micrograms/L wastewater',
             8: 'log10 micrograms/L wastewater',
             9: 'micrograms/g wet sludge',
             10: 'log10 micrograms/g wet sludge',
             11: 'micrograms/g dry sludge',
             12: 'log10 micrograms/g dry sludge'
            }
    
    conc_method = {
             'mf-mgcl2': 'membrane filtration with addition of mgcl2',
             'mf-acid': 'membrane filtration with sample acidification',
             'mf-acid-mgcl2': 'membrane filtration with acidification and mgcl2',
             'mf': 'membrane filtration with no amendment',
             'mf-mgcl2-addsolids': 'membrane filtration with addition of mgcl2, membrane recombined with separated solids',
             'mf-acid-addsolids': 'membrane filtration with sample acidification, membrane recombined with separated solids',
             'mf-acid-mgcl2-addsolids': 'membrane filtration with acidification and mgcl2, membrane recombined with separated solids',
             'mf-addsolids': 'membrane filtration with no amendment, membrane recombined with separated solids',
             'peg': 'peg precipitation',
             'ultracentrifugation': 'ultracentrifugation',
             'skimmilk': 'skimmed milk flocculation',
             'beefextract': 'beef extract flocculation',
             'promega-tna': 'promega wastewater large volume tna capture kit',
             'uf-centricon': 'centricon ultrafiltration',
             'uf-amicon': 'amicon ultrafiltration',
             'uf-hf-deadend': 'hollow fiber dead end ultrafiltration',
             'uf-innovaprep': 'innovaprep ultrafiltration',
             'noconc-addsolids': 'no liquid concentration, liquid recombined with separated solids',
             '13': 'none'}
    
    extract_meth = {
            'qiagen-viral': 'qiagen allprep powerviral dna/rna kit',
            'qiagen-fecal': 'qiagen allprep powerfecal dna/rna kit',
            'qiagen': 'qiagen allprep dna/rna kit',
            'qiagen-rneasy-power': 'qiagen rneasy powermicrobiome kit',
            'qiagen-powerwater': 'qiagen powerwater kit',
            'qiagen-rneasy': 'qiagen rneasy kit',
            'qiagen-qiaamp-epoch': 'qiagen qiaamp buffers with epoch columns',
            'promega-ht-tna': 'promega ht tna kit',
            'promega-ht-auto': 'promega automated tna kit',
            'promega-manual-tna': 'promega manual tna kit',
            'promega-ww-largevol-tna': 'promega wastewater large volume tna capture kit',
            'nuclisens-auto-magbead': 'nuclisens automated magnetic bead extraction kit',
            'phenol': 'phenol chloroform',
            'chemagic300': 'chemagic viral dna/rna 300 kit',
            'trizol-zymomagbeads-zymo': 'trizol, zymo mag beads w/ zymo clean and concentrator',
            '4smethod': '4s method',
            'zymoquick-r2014': 'zymo quick-rna fungal/bacterial miniprep #r2014',
            'magmax': 'thermo magmax microbiome ultra nucleic acid isolation kit',
            'none': 'none'}
    

    # Changing REDCap key:values for 'pretreatment'
    df_pid171['pretreatment'] = df_pid171['pretreatment'].map(pretreatment)

    #Changing REDCap key:values for "sars_cov2_units"
    df_pid171["sars_cov2_units"] = df_pid171["sars_cov2_units"].map(units)

    # Changing REDCap key:values for "hum_frac_mic_unit"
    df_pid171['hum_frac_mic_unit'] = df_pid171['hum_frac_mic_unit'].map(hum_frac_mic_unit)

    # Changing REDCap key:values for "other_norm_unit" #same key values as hum_frac_chem_units
    df_pid171['other_norm_unit'] = df_pid171['other_norm_unit'].map(hum_frac_chem_unit)
    
    # Changing REDCap key:value for "concentration_method"
    df_pid171["concentration_method"] = df_pid171["concentration_method"].map(conc_method)
    
    # Changing REDCap key:value for extraction_method
    df_pid171["extraction_method"] = df_pid171["extraction_method"].map(extract_meth)
    
    
    #DCIPHER TRANSFORM: change "pre_conc_store_temp" values from "0-8C" to 4 and change column to float
    df_pid171["pre_conc_storage_temp"] = df_pid171["pre_conc_storage_temp"].map({'0-8C': 4})
    df_pid171["pre_conc_storage_temp"] = df_pid171["pre_conc_storage_temp"].astype(np.float64)
    
    #Convert limit of detection to float and in units of copies per Liter
    df_pid171["lod_sewage"] = df_pid171["lod_sewage"].map({'10,000 Copies/mL': 10000000,
                                                          '3400 Copies/mL': 3400000})
    
    return df_pid171


def pid176_transform(df_pid176):
    """
    transform values in select fields from REDCap PID171
    """

    df_pid176 = df_pid176.copy()
    
    pretreatment = {
            1:"yes",
            0:"no"
            }

    rec_eff = {    
            1: 'bcov vaccine',
            2: 'brsv vaccine',
            3: 'murine coronavirus',
            4: 'oc43',
            5: 'phi6',
            6: 'puro',
            7: 'ms2 coliphage',
            8: 'hep g armored rna'
            }

    spike_matrix = {
            1: 'raw sample',
            2: 'raw sample post pasteurization',
            3: 'clarified sample',
            4: 'sample concentrate',
            5: 'lysis buffer'
            }

    pcr_type = {
            1: 'qpcr',
            2: 'ddpcr',
            3: 'qiagen dpcr',
            4: 'fluidigm dpcr',
            5: 'life technologies dpcr',
            6: 'raindance dpcr'
            }

    hum_frac_target_mic = {
            1: 'pepper mild mottle virus',
            2: 'crassphage',
            3: 'hf183'
            }

    other_norm_name = {
            1: 'pepper mild mottle virus',
            2: 'crassphage',
            3: 'hf183',
            4: 'caffeine',
            5: 'creatinine',
            6: 'sucralose',
            7: 'ibuprofen'
            }

    num_no_target_control = {
             0: "0",   
             1: '1',
             2: '2',
             3: '3',
             4: 'more than 3',
            }

    hum_frac_chem_unit = {
             1: 'copies/L wastewater',
             2: 'log10 copies/L wastewater',
             3: 'copies/g wet sludge',
             4: 'log10 copies/g wet sludge',
             5: 'copies/g dry sludge',
             6: 'log10 copies/g dry sludge',
             7: 'micrograms/L wastewater',
             8: 'log10 micrograms/L wastewater',
             9: 'micrograms/g wet sludge',
             10: 'log10 micrograms/g wet sludge',
             11: 'micrograms/g dry sludge',
             12: 'log10 micrograms/g dry sludge'
            }

    # Changing REDCap key:values for 'pasteurized'
    df_pid176['pasteurized'] = df_pid176['pasteurized'].map(pretreatment)

    # Changing REDCap key:values for "rec_eff_target_name"
    df_pid176["rec_eff_target_name"] = df_pid176["rec_eff_target_name"].map(rec_eff)

    # Changing REDCap key:values for "rec_eff_spike_matrix"
    df_pid176['rec_eff_spike_matrix'] = df_pid176['rec_eff_spike_matrix'].map(spike_matrix)

    # Changing REDCap key:values for "pcr_type"
    df_pid176['pcr_type'] = df_pid176['pcr_type'].map(pcr_type)

    # Changing REDCap key:values for "hum_frac_target_mic"
    df_pid176['hum_frac_target_mic'] = df_pid176['hum_frac_target_mic'].map(hum_frac_target_mic)

    # Changing REDCap key:values for "other_norm_name"
    df_pid176['other_norm_name'] = df_pid176['other_norm_name'].map(other_norm_name)

    # Changing REDCap key:values for "num_no_target_control"
    df_pid176['num_no_target_control'] = df_pid176['num_no_target_control'].map(num_no_target_control)

    # Changing REDCap key:values for "hum_frac_chem_unit"
    df_pid176['hum_frac_chem_unit'] = df_pid176['hum_frac_chem_unit'].map(hum_frac_chem_unit)
    
    
    #Converting from int to float
    # first convert to numeric and coerce errors, downcast to float32, then change to float64 for consistency

    df_pid176["rec_eff_spike_conc"] = pd.to_numeric(df_pid176["rec_eff_spike_conc"], downcast = "float", errors = "coerce")
    df_pid176["rec_eff_spike_conc"] = df_pid176["rec_eff_spike_conc"].astype(np.float64) 
    
    return df_pid176

def DCIPHER_v3_modifications(complete):
    """
    DCIPHER has made major changes in update 3.0, modifications to adjust to changes. 
    
    -rename columns,
    -add "pcr_gene_target' field to contain values from "pcr_target" field
    -change all values in "pcr_target" to "sars-cov-2"
    
    """
    
    complete = complete.copy()
    
    
    # 5 additional fields that DCIPHER expects (prior update)
    complete[['analysis_ignore', 'dashboard_ignore', 'major_lab_method', 'major_lab_method_desc', 'qc_ignore']] = np.nan
    
    #renaming fields
    names_dict = {
    'sars_cov2_units':'pcr_target_units',
    'sars_cov2_avg_conc':'pcr_target_avg_conc',
    'sars_cov2_std_error':'pcr_target_std_error',
    'sars_cov2_cl_95_lo':'pcr_target_cl_95_lo',
    'sars_cov2_cl_95_up':'pcr_target_cl_95_up',
    'sars_cov2_below_lod':'pcr_target_below_lod'}

    complete = complete.rename(columns = names_dict)
    
    #adding 'pcr_gene_target'
    complete['pcr_gene_target'] = complete["pcr_target"]
    
    #setting all pcr_target values to "sars-cov-2"
    complete["pcr_target"] = "sars-cov-2"
    
    return complete