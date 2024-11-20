FROM bitnami/spark:latest


# Switch to root user to install dependencies
USER root


# Update and install Python 3, pip, and required libraries
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install numpy streamlit && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# Switch back to the default user
USER 1001