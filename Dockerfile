FROM rust:1.84-alpine as builder

# Install build dependencies required for compilation
RUN apk add --no-cache musl-dev openssl-dev

# Set the working directory inside the container
WORKDIR /app

# Copy the entire project into the container
COPY . .

# Install project dependencies and build the project with the desired feature
RUN cargo build --release --features cloud_server

# Use a minimal Alpine base image for the final stage
FROM alpine:3.18

# Install runtime dependencies required for the binary
RUN apk add --no-cache libssl3

# Set the working directory inside the container
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/tsdb_timon /usr/local/bin/tsdb_timon

# Set the default command to run the app
CMD ["tsdb_timon", "--features", "cloud_server"]

# Expose the necessary port
EXPOSE 8080
