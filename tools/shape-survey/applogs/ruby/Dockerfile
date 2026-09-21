FROM ruby:3.3-slim

# Pinned, and both pure-Ruby, so this installs in seconds with no build toolchain. `webrick` left
# the stdlib in Ruby 3.0, which is why it is a dependency at all; see app.rb's header for why
# there is no Rails (and therefore no lograge) here.
RUN gem install --no-document semantic_logger:5.1.0 webrick:1.9.2

WORKDIR /app
COPY app.rb /app/
