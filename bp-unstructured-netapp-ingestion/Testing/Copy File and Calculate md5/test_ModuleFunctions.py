pip install pytest
import pytest
import re
from io import StringIO
import sys
def print_boundary(num_of_dashes: int):
  print('-'*num_of_dashes)


###################################################################################################################################
############################################################## TESTS ##############################################################
###################################################################################################################################


def test_print_boundary():
    num_of_dashes = 5
    expected_output = '-' * num_of_dashes + '\n'
    
    # Redirect stdout to capture print output
    sys.stdout = StringIO()
    
    # Call the function
    print_boundary(num_of_dashes)
    
    # Capture printed output
    captured = sys.stdout.getvalue()
    
    # Restore stdout
    sys.stdout = sys.__stdout__
    
    # Compare captured output with expected output
    assert captured == expected_output
    
test_print_boundary()
def get_selected_column(query: str):
  '''
  Extracts and returns the single column name specified in a SQL SELECT statement from the provided query string.

  Parameters:
  query (str): A SQL query string containing a SELECT statement with exactly one column.

  Returns:
  str: The name of the selected column.

  Raises:
  Exception: If the query specifies more than one column, indicated by the presence of a comma.
  Exception: If the column name contains dot notation, indicating a qualified column reference.
  Exception: If the query uses "SELECT *", indicating all columns are selected.

  Example:
  >>> get_selected_column("SELECT column_name FROM table_name")
  'column_name'
  '''
  parts = query.lower().split('select ', 1)[1].split(' from', 1)
  column = parts[0].strip()

  if column == '*':
    raise Exception('⚠️ Exception in get_selected_column() function: the select query cannot be "SELECT *" - specify one column ⚠️')
  elif ',' in column:
    raise Exception(f'⚠️ Exception in get_selected_column() function: the select query cannot include commas - specify 1 column at most ⚠️')
  elif '.' in column:
    raise Exception(f'⚠️ Exception in get_selected_column() function: the select query cannot include dot notation in the column selection; remove the full stop ⚠️')
  else:
    return column
      

###################################################################################################################################
############################################################## TESTS ##############################################################
###################################################################################################################################


def test_get_selected_column_basic():
  query = "SELECT column_name FROM table_name"
  expected_result = "column_name"
  
  try:
    result = get_selected_column(query)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_get_selected_column_with_comma():
  query = 'SELECT column1, column2 FROM table_name'
  expected_message = "⚠️ Exception in get_selected_column() function: the select query cannot include commas - specify 1 column at most ⚠️"
  escaped_message = re.escape(expected_message) # Otherwise recognises special characters in expected exception message string and interprets it as a regular expression pattern. Since you intend to match the exact string rather than use regex functionality, you can address this by escaping special characters using Python's re.escape() function.

  with pytest.raises(Exception, match=escaped_message):
    get_selected_column(query)


def test_get_selected_column_with_dot():
  query = 'SELECT table.column_name FROM table_name'
  expected_message = "⚠️ Exception in get_selected_column() function: the select query cannot include dot notation in the column selection; remove the full stop ⚠️"
  escaped_message = re.escape(expected_message)

  with pytest.raises(Exception, match=escaped_message):
    get_selected_column(query)


def test_get_selected_column_with_asterix():
  query = 'SELECT * FROM table_name'
  expected_message = '⚠️ Exception in get_selected_column() function: the select query cannot be "SELECT *" - specify one column ⚠️'
  escaped_message = re.escape(expected_message)

  with pytest.raises(Exception, match=escaped_message):
    get_selected_column(query)


def test_get_selected_column_with_spaces():
  query = '    SELECT column_name FROM table_name'
  expected_column = "column_name"
  
  try:
    result = get_selected_column(query)
    assert result == expected_column
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: {str(e)}")


test_get_selected_column_basic()
test_get_selected_column_with_comma()
test_get_selected_column_with_dot()
test_get_selected_column_with_asterix()
test_get_selected_column_with_spaces()
def get_exclusion_user_variable(userInput):
    '''
    Processes the user input to generate a set of exclusion variables.

    Parameters:
    userInput (str): The input argument, which can be:
                     - An empty string
                     - A string of comma-separated values (e.g., "A,B,C")
                     - A simple SELECT column SQL query (e.g., "SELECT col FROM db.tbl"), where either all values of the selected column are returned, or all comma-separated strings in the column value are returned.  

    Returns:
    set: An empty set if the input is an empty string.
         A set of strings from the results of the input SELECT query.
         A set of strings if the input is a string of comma-separated values.

    Raises:
    Exception: If an error occurs during processing, prints an error message.

    Example:
    >>> get_exclusion_user_variable("")
    {''}
    >>> get_exclusion_user_variable("A,B,C")
    {'A', 'B', 'C'}
    >>> get_exclusion_user_variable("SELECT col FROM db.tbl")
    {'result1', 'result2', ...}  # Assuming these are the results from the query.
    '''
    try:
        userInput = userInput.lower().strip()
        if not userInput:
            return {''}
        elif userInput.startswith('select') and ' from ' in userInput:
            column = get_selected_column(userInput)
            spark.conf.set("spark.sql.execution.arrow.enabled", "false")
            originalSet = set(spark.sql(userInput).select(column).toPandas()[column])
            split_set = {item for s in originalSet for item in s.split(',')}
            return split_set
        else:
            return set(userInput.split(','))
    except Exception as e:
        print(f'⚠️ Exception in get_exclusion_user_variable() function: {e}.\n')
        # Re-raise the exception to propagate it to the caller
        raise


###################################################################################################################################
############################################################## TESTS ##############################################################
###################################################################################################################################


def test_get_exclusion_user_variable_basic():
  query = "seLEct filePath from unstruct_metadata.exclusion_filepath where jobGroup=599 and jobOrder=3 limit 1"
  expected_result = {'aadsmbuswp-895a.bp1.ad.bp.com/usw2/hou_GoM_SPU/Resource/AREA/Thunder_Horse/_Hub/Core_Documents/Area_Dev_Plan/2018 ADP/Sharepoint_Content/Sprint Packs/ThunderHorse_2018_ADP_HoF_Aug27_DRAFT.docx'}
  
  try:
    result = get_exclusion_user_variable(query)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_get_exclusion_user_variable_basic2():
  query = "seLEct abbreviation from unstruct_metadata.exclusion_abbreviation where jobGroup=599 and jobOrder=3"
  expected_result = {'ADP',
 'AFE',
 'Area Development Plan',
 'Area Resource Progression Plan',
 'Authorisation for Expenditure',
 'BPE',
 'Budget',
 'Business Plan',
 'Contractors Performance',
 'EIA',
 'Environmental Impact Assessment',
 'FM',
 'Finance Memorandum',
 'JOA',
 'Joint Operating Agreements',
 'PSA',
 'PSC',
 'Production Sharing Agreement',
 'Production Sharing Contract',
 'QPF',
 'RAM',
 'RDP',
 'RPP',
 'RSP',
 'Regional Development Plan',
 'Reserve Review Summary',
 'Resource Approval Memorandum',
 'Resource Support Package',
 'Subsurface Hopper',
 'TAM',
 'Technical Assurance Memorandum',
 'Tender Document'}
  
  try:
    result = get_exclusion_user_variable(query)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_get_exclusion_user_variable_with_comma():
  query = 'SELECT column1, column2 FROM table_name'
  expected_message = "⚠️ Exception in get_selected_column() function: the select query cannot include commas - specify 1 column at most ⚠️"
  escaped_message = re.escape(expected_message) # Otherwise recognises special characters in expected exception message string and interprets it as a regular expression pattern. Since you intend to match the exact string rather than use regex functionality, you can address this by escaping special characters using Python's re.escape() function.

  with pytest.raises(Exception, match=escaped_message):
    get_exclusion_user_variable(query)


def test_get_exclusion_user_variable_with_dot():
  query = 'SELECT table.column_name FROM table_name'
  expected_message = "⚠️ Exception in get_selected_column() function: the select query cannot include dot notation in the column selection; remove the full stop ⚠️"
  escaped_message = re.escape(expected_message)

  with pytest.raises(Exception, match=escaped_message):
    get_exclusion_user_variable(query)


def test_get_exclusion_user_variable_with_asterix():
  query = 'SELECT * FROM table_name'
  expected_message = '⚠️ Exception in get_selected_column() function: the select query cannot be "SELECT *" - specify one column ⚠️'
  escaped_message = re.escape(expected_message)

  with pytest.raises(Exception, match=escaped_message):
    get_exclusion_user_variable(query)


test_get_exclusion_user_variable_basic()
test_get_exclusion_user_variable_basic2()
test_get_exclusion_user_variable_with_comma()
test_get_exclusion_user_variable_with_dot()
test_get_exclusion_user_variable_with_asterix()
def item_in_collection(item, collection):
  '''
  Checks if the input item is in the provided collection.

  Parameters:
  item (str): The item to check.
  collection (set/list/series): The collection of items.

  Returns:
  bool: True if the item is in the collection, False otherwise.

  Raises:
  Exception: If an error occurs during processing, prints an error message.

  Example:
  >>> item_in_collection("apple", {"apple", "banana"})
  True
  >>> item_in_collection("cherry", {"apple", "banana"})
  False
  >>> item_in_collection("apple", [""])
  False
  '''
  try:
    collectionSet = set(collection)
    itemSet = set([item])
    return bool(itemSet & collectionSet)
  except Exception as e:
    print(f'⚠️ Exception in item_in_collection() function: {e}.\n')


###################################################################################################################################
############################################################## TESTS ##############################################################
###################################################################################################################################


def test_item_in_collection_basic():
  filePath = 'abc/def/ghi'
  filePathsToExclude = ['abc/def/ghi','xyz/abc']
  expected_result = True

  try:
    result = item_in_collection(filePath, filePathsToExclude)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_item_in_collection_basic2():
  '''
  Expected boolean is False because the filePathsToExclude='abc/def/ghi' is split into a set --> {'/', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}
  '''
  filePath = 'abc/def/ghi'
  filePathsToExclude = 'abc/def/ghi'
  expected_result = False

  try:
    result = item_in_collection(filePath, filePathsToExclude)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_item_in_collection_basic3():
  '''
  '''
  filePath = 'abc/def/ghi'
  filePathsToExclude = ''
  expected_result = False

  try:
    result = item_in_collection(filePath, filePathsToExclude)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


test_item_in_collection_basic()
test_item_in_collection_basic2()
test_item_in_collection_basic3()
def term_in_string(terms, string):
  '''
  Checks if the input string contains any terms or abbreviations from the provided collection of terms to be excluded.

  Parameters:
  string (str): The string to check.
  terms (list/set/str): The collection of terms or abbreviations to check against.

  Returns:
  bool: True if the string contains any of the terms, False otherwise.

  Raises:
  Exception: If an error occurs during processing, prints an error message.

  Example:
  >>> term_in_string(["confidential", "secret"], "confidential_report")
  True
  >>> term_in_string(["confidential", "secret"], "public_report")
  False
  '''
  import re
  try:
    if terms in ([''], {''}, ''):
        return False
    elif len(terms) > 0 and re.compile('|'.join(terms), re.IGNORECASE).search(string):
        return True
    else:
        return False
  except Exception as e:
    print(f'⚠️ Exception in term_in_string() function: {e}.\n')


###################################################################################################################################
############################################################## TESTS ##############################################################
###################################################################################################################################


def test_term_in_string_basic():
  fileNamesToExclude = ['hello','meep']
  fileName = 'hello_there'
  expected_result = True

  try:
    result = term_in_string(fileNamesToExclude, fileName)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


def test_term_in_string_basic2():
  fileNamesToExclude = ['hello','meep']
  fileName = 'this_file_here'
  expected_result = False

  try:
    result = term_in_string(fileNamesToExclude, fileName)
    assert result == expected_result
  except Exception as e:
    raise AssertionError(f"Unexpected exception raised: expected result is {expected_result}, but result is {result}. {str(e)}")


test_term_in_string_basic()
test_term_in_string_basic2()
