# Import pandas
import pandas as pd
def from_DOI(f_doi, doi):
    """
    from doi get the MPI_ID, then from MPI_ID get ou_ID and ctx_ID
    f_doi contains mapping from the input doi to MPI_ID
    the mapping from MPI_ID to ou_ID and ctx_ID should be relatively constant, directly being given here
    """

    df_DOI = pd.read_csv(f_doi, sep = ';', header = None, names = ['DOI','MPI'], index_col=False, usecols=[0,3])
    list_DOI_lower = [item.lower() for item in df_DOI.get('DOI').values] # to avoid case sensitive when searching for the string
    try:
        DOI_ind = list_DOI_lower.index(doi.lower())
    except ValueError: 
        # deal with the occasion that the doi is not in the list
        return 'xxx', 'xxx'
    MPI_ID = df_DOI.get('MPI').values[DOI_ind]

    f_MPI = "subsidiary_doc/instId_ctxId.xlsx"
    df_MPI = pd.read_excel(f_MPI, sheet_name = 0, usecols = [0,1,2])
    MPI_ind = list(df_MPI.get('MPI-ID').values).index(MPI_ID)
    CTX = df_MPI.get('Context-ID').values[MPI_ind]
    OU = df_MPI.get('OU-ID').values[MPI_ind]
    return CTX, OU

def pyExlDict(filexl):
    """
    process a two column .xlsx file
    input: filexl: file name in <str>
    output: dictout: a dict with column 1 as key, column 2 as value
    """
    dtfr = pd.read_excel(filexl, dtype = str)
    key_list = list(dtfr.keys())
    dictout = {}
    col1 = dtfr.get(key_list[0])
    col2 = dtfr.get(key_list[1])
    for i in range(len(col1)):
        dictout[col1[i]] = col2[i]

    return dictout
