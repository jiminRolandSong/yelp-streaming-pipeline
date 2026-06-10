import streamlit as st


def render_sidebar() -> None:
    st.markdown("""
<style>
[data-testid="stSidebarNav"] { display: none; }
[data-testid="stSidebarNavItems"] { display: none; }
</style>
""", unsafe_allow_html=True)

    st.sidebar.title("Yelp Analytics")
    st.sidebar.markdown("---")
    st.sidebar.page_link("app.py", label="Home", icon="🏠")
    st.sidebar.page_link("pages/overview.py", label="Overview", icon="📈")
    st.sidebar.page_link("pages/insights.py", label="AI Insights", icon="🤖")
    st.sidebar.page_link("pages/pipeline.py", label="Pipeline Status", icon="🔧")
