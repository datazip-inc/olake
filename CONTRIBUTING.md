# Contributing to OLake

We truly value and appreciate all contributions — every effort makes a difference, and proper credit will always be given.  
To ensure consistency, we follow a structured contribution process. All guidelines and details have been consolidated into the official OLake documentation. Please refer to the links below for more information:

- [Contribute To OLake](https://olake.io/docs/community/contributing)
- [How To Raise A PR](https://olake.io/docs/community/issues-and-prs)
- [Setting Up A Development Environment](https://olake.io/docs/community/setting-up-a-dev-env)

---

## 🎃 Hacktoberfest 2025 @ OLake

OLake is officially open for **Hacktoberfest contributions**! 🚀

If you’re participating in Hacktoberfest, look out for any issues labeled:

- **`hacktoberfest`**
- **`good first issue`**

These are designed to help new contributors get started quickly.  
We welcome everything — bug fixes, documentation updates, tests, or feature enhancements.

👉 [Check our open issues here](../../issues)

Let’s hack, learn, and grow together this Hacktoberfest. Happy contributing & happy engineering! ⚡

---

## 🔐 Testing SSH Tunnel Support (Oracle & MongoDB)

OLake drivers for **Oracle** and **MongoDB** now support SSH tunneling via a local port-forwarded connection.

To test this feature:

1. Generate a dummy PEM key:

   ```bash
   ssh-keygen -t rsa -b 2048 -m PEM -f test_key.pem -N ""

   ```

2. Place the key in the appropriate driver folder:
   drivers/oracle/internal/test_key.pem
   drivers/mongodb/internal/test_key.pem

3. Run the tests:
   go test -v -run TestOracleSSHConnection
   go test -v -run TestMongoSSHConnection

These tests are designed to fail gracefully if no SSH server is running on localhost:2222, but they validate:

- SSH key parsing
- Tunnel setup logic
- DSN override behavior

This ensures contributors can safely test SSH logic without needing a real bastion.

## Getting Help

For any questions, concerns, or queries:

- Start a discussion on our [**Slack**](https://olake.io/slack/) channel.
- Check [GitHub Discussions](https://github.com/datazip-inc/olake/discussions) for ongoing conversations.
- Don’t hesitate to open an issue if something is unclear.

---

### 💡 We look forward to your feedback and contributions to improve this project!

<!----variables---->

[CLA]: https://docs.google.com/forms/d/e/1FAIpQLSdze2q6gn81fmbIp2bW5cIpAXcpv7Y5OQjQyXflNvoYWiO4OQ/viewform
