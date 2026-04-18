import streamlit as st
import requests

# Backend endpoint (we'll build this next)
BACKEND_URL = "http://backend:8000/run-pipeline"

st.title("📈 Financial Data Platform")
st.write("THIS IS MY NEW PROJECT")


# Stock selection
available_stocks = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

symbols = st.multiselect(
    "Select Stocks",
    available_stocks,
    default=["AAPL"]
)

# Run button
if st.button("🚀 Run Pipeline"):
    if not symbols:
        st.warning("Please select at least one stock")
    else:
        try:
            response = requests.post(
                BACKEND_URL,
                json={"symbols": symbols}
            )

            if response.status_code == 200:
                st.success("Pipeline triggered successfully!")
            else:
                st.error("Failed to trigger pipeline")

        except Exception as e:
            st.error(f"Error: {e}")
