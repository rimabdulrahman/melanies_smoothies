# Import python packages
import streamlit as st
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import requests

# Write directly to the app
st.title(f"Pending Smootie Orders :cup_with_straw: {st.__version__}")
st.write(
  """Orders that need to be filled
  """
)


#session = get_active_session()
cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect() 
#st.dataframe(data=my_dataframe, use_container_width=True)

if my_dataframe:
    editable_df = st.data_editor(my_dataframe)
    submitted = st.button('Submit')
    
    if submitted:
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
        
        try:
            og_dataset.merge(edited_dataset
                ,(og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                ,[when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order updated!', icon="👍")
    
        except:
            st.success('Smothing went wrong!')
else:
    st.success('There are no penfing orders right now')
    


