FROM ruby:3.3-slim

# Pinned and pure-Ruby, so no build toolchain. `webrick` left the stdlib in Ruby 3.0. See app.rb's
# header for why there is no Rails or lograge.
RUN gem install --no-document semantic_logger:5.1.0 webrick:1.9.2

WORKDIR /app
COPY app.rb /app/
