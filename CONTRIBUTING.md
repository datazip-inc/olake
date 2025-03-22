# Contributing to Olake

Thanks for taking the time and for your help in improving this project!

## Table of contents
- [Olake Framework Structure](#olake-framework-structure)
- [Olake Contributing Agreement](#olake-contributor-agreement)
- [How You Can Contribute to Olake](#how-you-can-contribute-to-olake)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Committing](#committing)
- [Contributors](#contributors)
- [Installing and Setting Up Olake](#installing-and-setting-up-olake)
- [Getting Help](#getting-help)

## Olake Framework Structure
![diagram](/.github/assets/Olake.jpg)

## Olake Contributor Agreement

To contribute to this project, we need you to sign the [**Contributor License Agreement (“CLA”)**][CLA] for the first commit you make. By agreeing to the [**CLA**][CLA] we can add you to the list of approved contributors and review the changes proposed by you.

## How you can contribute to Olake

You can contribute to the open-source Olake project. View our [**Issues Page**](https://github.com/datazip-inc/olake/issues) to see all open issues. If you encounter a bug or have an improvement suggestion, you can [**submit an issue**](https://github.com/datazip-inc/olake/issues/new) describing your proposed change.

One way you can contribute to Olake is to create a Driver. A Driver is a connection between Olake and a Database from where you would like to fetch your event data. There are several reasons why you may want to build a Driver:

- Custom Database Support: If your organization uses a unique or proprietary database not currently supported by Olake, creating a Driver allows seamless integration and data ingestion.
- Optimized Performance: By building a custom Driver, you can tailor it to optimize for your specific use case, improving performance and reducing latency during data fetch operations.
- Advanced Features: Implement additional capabilities, such as support for custom queries, change data capture, or enhanced data transformation, to better suit your data workflows.

For more information on the different ways in which you can contribute to Olake, you can chat with us on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) channel.

## Submitting a pull request

The type of change you make will dictate what repositories you will need to make pull requests for. You can reach out to us on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ/) channel if you have any questions.

For example, to contribute a new driver, you need to create a pull request (PR). Follow these steps to ensure your PR is well-prepared:
- Provide a clear and concise PR title.
- Write a detailed and descriptive PR description.
- Request a code review from the maintainers.

## Committing

Before adding any changes, please pull the latest code from the remote repository to ensure you're working with the most recent version. Once your work is ready, follow these steps:

1. **Checkout to the staging branch:**  
   Run `git checkout staging` to switch to the staging branch.

2. **Pull the latest changes:**  
   Run `git pull` to update your local staging branch with any new changes from the remote repository.

3. **Stage, commit, and push your changes:**  
   Stage your changes using `git add .`, then commit with a clear message using `git commit -m "Your commit message"`, and finally push to the staging branch with `git push`.

We prefer squash or rebase commits so that all changes from a branch are committed to master as a single commit. All pull requests are squashed when merged, but rebasing prior to merge gives you better control over the commit message. Only signed commits are accepted for contribution.

## Contributors

We appreciate everyone who has contributed to Olake. The list below is automatically generated. If you'd like to add your name, please follow the instructions provided by our automation tool (for example, using the [all‑contributors](https://allcontributors.org/) bot).

<!-- ALL-CONTRIBUTORS-LIST:START -->
<!-- The following section is automatically updated with the list of contributors -->
<!-- Example entry: -->
<!--
<table>
  <tr>
    <td align="center">
      <a href="https://github.com/username">
        <img src="https://avatars.githubusercontent.com/username?s=100" width="100px;" alt=""/><br />
        <sub><b>username</b></sub>
      </a>
    </td>
  </tr>
</table>
-->
<!-- ALL-CONTRIBUTORS-LIST:END -->

## Installing and setting up Olake

To contribute to this project, you may need to install Olake on your machine. You can do so by following our [**docs**](https://olake.io/docs) and set up Olake in no time.

## Getting help

For any questions, concerns, or queries, you can start by asking a question on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) channel.
<br><br>

### We look forward to your feedback on improving this project!

<!----variables---->

[CLA]: https://docs.google.com/forms/d/e/1FAIpQLSdze2q6gn81fmbIp2bW5cIpAXcpv7Y5OQjQyXflNvoYWiO4OQ/viewform
