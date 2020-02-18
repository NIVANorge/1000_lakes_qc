def ion_balance(df, fillna=False):
    """
    """
    import pandas as pd
    
    if fillna:
        df.fillna(0, inplace=True)
        
    # Cations
    df['Kalsium-UEQ_P_L']   = df['Kalsium-MG_P_L']   * 2000 / 40.078
    df['Magnesium-UEQ_P_L'] = df['Magnesium-MG_P_L'] * 2000 / 24.305
    df['Natrium-UEQ_P_L']   = df['Natrium-MG_P_L']   * 1000 / 22.990
    df['Kalium-UEQ_P_L']    = df['Kalium-MG_P_L']    * 1000 / 39.100
    df['Ammonium-UEQ_P_L']  = df['Ammonium-UG_N_L']  / 14.010
    df['H-UEQ_P_L']         = 1E6 * 10**-df['pH-PH']
    df['Aluminium-UG_P_L']  = (df['Aluminium, reaktivt-UG_P_L'] - 
                               df['Aluminium, ikke labil-UG_P_L']) * 3 / 26.980

    df['cations'] = (df['Kalsium-UEQ_P_L']   +
                     df['Magnesium-UEQ_P_L'] +
                     df['Natrium-UEQ_P_L']   +
                     df['Kalium-UEQ_P_L']    +
                     df['Ammonium-UEQ_P_L']  +
                     df['H-UEQ_P_L']         +
                     df['Aluminium-UG_P_L']
                    )
    
    # Anions
    df['Klorid-UEQ_P_L']           = df['Klorid-MG_P_L']    * 1000 / 35.453
    df['Sulfat-UEQ_P_L']           = df['Sulfat-MG_P_L']    * 2000 / 96.060
    df['Nitritt + nitrat-UEQ_P_L'] = df['Nitritt + nitrat-UG_N_L']   / 14.010
    df['Alkalitet-UEQ_P_L']        = ((df['Alkalitet-MMOLE_P_L'] * 1000 - 31.6) + 
                                      0.646 * (df['Alkalitet-MMOLE_P_L'] * 1000 - 31.6)**0.5)    
    df['A-UEQ_P_L']                = df['Total organisk karbon (TOC)-MG_C_L'] * (1.77 * df['pH-PH'] - 3)
    
    df['anions'] = (df['Klorid-UEQ_P_L']           +          
                    df['Sulfat-UEQ_P_L']           +
                    df['Nitritt + nitrat-UEQ_P_L'] +
                    df['Alkalitet-UEQ_P_L']        +
                    df['A-UEQ_P_L']
                   )
    
    # Percentage difference
    df['ion_diff_pct'] = (df['cations'] - df['anions']) * 100 / df['cations']
    
    # Kond difference
    df['KondT-MS_P_M'] = (0.0595 * df['Kalsium-UEQ_P_L']          +
                          0.0530 * df['Magnesium-UEQ_P_L']        +
                          0.0501 * df['Natrium-UEQ_P_L']          +
                          0.0735 * df['Kalium-UEQ_P_L']           +
                          0.0735 * df['Ammonium-UEQ_P_L']         +
                          0.0763 * df['Klorid-UEQ_P_L']           +  
                          0.0800 * df['Sulfat-UEQ_P_L']           +
                          0.0714 * df['Nitritt + nitrat-UEQ_P_L'] +
                          0.0445 * df['Alkalitet-UEQ_P_L']        +
                          0.3500 * df['H-UEQ_P_L']                +
                          0.0610 * df['Aluminium-UG_P_L']
                         ) / 10
    
    df['kond_diff_pct'] = (df['KondT-MS_P_M'] - df['Konduktivitet-MS_P_M']) * 100 / df['KondT-MS_P_M']
    
    # Kond - (H + SO4)
    df['kond_minus_H&SO4'] = (df['Konduktivitet-MS_P_M'] - 
                              (0.035 * df['H-UEQ_P_L']) - 
                              (0.008 * df['Sulfat-UEQ_P_L'])
                             ) 
    
    # Get cols of interest
    idx_cols = ['station_id', 'station_name', 'sample_date']
    cols = ['pH-PH',
            'Kalsium-MG_P_L',
            'Magnesium-MG_P_L',
            'Natrium-MG_P_L',
            'Kalium-MG_P_L',
            'Ammonium-UG_N_L',
            'Aluminium, reaktivt-UG_P_L',
            'Aluminium, ikke labil-UG_P_L',
            'Klorid-MG_P_L',
            'Sulfat-MG_P_L',
            'Nitritt + nitrat-UG_N_L',
            'Alkalitet-MMOLE_P_L',
            'Total organisk karbon (TOC)-MG_C_L',
            'Kalsium-UEQ_P_L',
            'Magnesium-UEQ_P_L',
            'Natrium-UEQ_P_L',
            'Kalium-UEQ_P_L',
            'Ammonium-UEQ_P_L',
            'H-UEQ_P_L',
            'Aluminium-UG_P_L',
            'Klorid-UEQ_P_L',
            'Sulfat-UEQ_P_L',
            'Nitritt + nitrat-UEQ_P_L',
            'Alkalitet-UEQ_P_L',
            'A-UEQ_P_L',
            'cations',
            'anions',
            'ion_diff_pct',
            'kond_diff_pct',
            'kond_minus_H&SO4',
           ]
    
    return df[idx_cols + cols]           