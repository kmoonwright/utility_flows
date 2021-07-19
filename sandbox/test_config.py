import prefect

def write_error_df_to_artifact() -> None:
    """
    Takes a dataframe and a string, and compiles them into a markdown report for appending to a meta_datapull flow run as an artifact.
    :param df: Pandas DataFrame containing data which is breaking a unit test
    :param error_msg: String message that should go into the meta_datapull artifact error report
    """
    prefect.artifacts.create_markdown('There was an error! Fix it')
    #prefect.artifacts.create_markdown('# Error Report\n' + error_msg + '\n' + df.to_markdown())
    return