# Trino with Olake

This guide walks you through how to use Trino to query your Iceberg tables, which are managed by the main Olake Docker stack. We'll cover the basic setup, what you need to know, and some common fixes.

### 1. Setting up Olake

Setting up Olake here are some common things you might need to set up for your main Olake project.

#### 1.1 Changing the Admin User

Olake automatically makes an admin user when it starts. The default username is `admin`. If you want to change the default username and password, you can edit this section in your `docker-compose.yml` file:

```yaml
x-signup-defaults: username: "&defaultUsername" password: "&defaultPassword"
