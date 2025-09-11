# Contributing to Olake

Thanks for taking the time and for your help in improving this project!

## Table of contents
- [Olake Framework Structure](#olake-framework-structure)
- [Olake Contributing Agreement](#olake-contributor-agreement)
- [How You Can Contribute to Olake](#how-you-can-contribute-to-olake)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Committing](#committing)
- [Installing and Setting Up Olake](#installing-and-setting-up-olake)
- [Getting Help](#getting-help)

## Olake Framework Structure
![diagram](/.github/assets/Olake.jpg)

## Setting Up and Running the OLake CLI Prior to Contribution

Before making contributions or actively using the project, we expect the community members typically attempt to run it. If the project itself is not straightforward to set up and execute, it can discourage participation and reduce contributor interest.

For more information, follow our [**docs**](https://olake.io/docs) and set up Olake in no time.

## Olake Contributor Agreement

To contribute to this project, we need you to sign the [**Contributor License Agreement (“CLA”)**][CLA] for the first commit you make. By agreeing to the [**CLA**][CLA]
we can add you to list of approved contributors and review the changes proposed by you.

## How you can contribute to Olake

**We are always very happy to have contributions, whether for trivial cleanups or big new features.**

You can contribute to the open-source Olake project by following these steps:
- View our [**Issues Page**](https://github.com/datazip-inc/olake/issues) to explore all open issues.
- If you are interested in contributing to any of the issue(s), either:
    - Comment directly on the issue, or
    - Inform us on [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) with the issue number you’d like to work on.
- A project moderator will review your request and assign the issue to you.
- Once your pull request (PR) is ready, reference the corresponding issue number in your PR description. A maintainer will then review your submission.
- If you discover a bug or would like to suggest an enhancement, you can [**submit an issue**](https://github.com/datazip-inc/olake/issues/new) with a clear description of the problem or proposed change.

For more information on the different ways in which you can contribute to Olake, you can also chat with us on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) channel.


## Submitting a pull request

For every suggested change or issue, you can make a pull request to the appropriate branch based on the type of change. You can reach out to us on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ/) channel if you have any questions.

**Best Practices for Raising a Pull Request (PR)**
- Title: Use a concise, descriptive title that summarizes the change.
- Description: Provide a detailed explanation of the change, including context, motivation, and references to the related issue number. Where applicable, include screenshots, logs, or examples.
- Branching: For code-related issues, if no target branch is specified, base your PR on the staging branch.
- Code Review: Request a review from the maintainers once your PR is ready.
- Clarity & Scope: Keep the PR focused on a single issue or logical set of changes to make reviews faster and cleaner.

## Committing

Make sure to pull the latest changes from the respective branch before committing. Run `make pre-commit` to validate your changes. Use a clear and descriptive commit message that reflects the purpose of your changes. Your pull request will be reviewed by the appropriate OLake maintainers.

## Debugging

There are some assumptions required to use debugging:
- You are using VSCode to run OLake locally.
- You have cloned the project in a suitable directory.

Steps to Debug:
- Make a directory `.vscode` (inside OLake project, at root location) if not already created.
- Create a file named `launch.json` inside the .vscode directory and paste the beflow config.
- Simply run the provided command within your chosen source, and you are good to go.

```json title="launch.json"
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Launch Go Code",
            "type": "go",
            "request": "launch",
            "program": "{PATH_TO_UPDATE}/drivers/mongodb/main.go",
            "mode": "auto",
            "args": [
                "sync",
                "--config",
                "{PATH_TO_UPDATE}/drivers/mongodb/examples/source.json",
                "--catalog",
                "{PATH_TO_UPDATE}/drivers/mongodb/examples/streams.json",
                "--destination",
                "{PATH_TO_UPDATE}/drivers/mongodb/examples/destination.json",
                // "--state",
                // "{PATH_TO_UPDATE}/drivers/mongodb/examples/state.json",
            ]
        }
    ]
}
```


## Getting help

For any questions, concerns, or queries, you can start by asking a question on our [**Slack**](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) channel.
<br><br>

### We look forward to your feedback on improving this project!


<!----variables---->

[CLA]: https://docs.google.com/forms/d/e/1FAIpQLSdze2q6gn81fmbIp2bW5cIpAXcpv7Y5OQjQyXflNvoYWiO4OQ/viewform
