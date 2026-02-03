# Start with a Linux system that has the GCC compiler
FROM gcc:latest

# Create a working directory inside the container
WORKDIR /app

# Copy your main.cpp from your Mac into the container
COPY main.cpp .

# Compile the code into an executable named 'mydb'
RUN g++ -o mydb main.cpp

# When the container starts, run the executable
CMD ["./mydb"]