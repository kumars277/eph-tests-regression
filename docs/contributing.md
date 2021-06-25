# Contribution Guidelines:

### Pull Requests

Please read the code review guidelines

    DO keep your pull request tightly scoped to a specific task.
    BECAUSE Small, focussed changes are easier to review and likely to get reviewed and integrated much faster.
    ------------------------------
    DO hand over pull requests you've opened to another team member if you're going to be away.
    BECAUSE You may have a headache integrating it later if the codebase moves on significantly in your absence.
    ------------------------------
    DO NOT merge other peoples pull requests without confirming with them.
    BECAUSE There may be a good reason they haven't merged it yet - if you need something merged urgently contact the person who opened the pull request.

### Git

    DO name your dev branch so it starts with the ticket number e.g. <JIRA_ID>_whatever-you-like. 
    BECAUSE So reviewers can refer back to the acceptance criteria on the ticket.
    ------------------------------
    DO rebase your dev branch on master before submitting for review.
    BECAUSE Ensures you are up to date with the latest state of the main branch and have resolved conflicts before submitting the pull request.
    How? 
    $ git rebase origin master
    ------------------------------
    DO delete your remote and local branches once merged.
    BECAUSE It will clutter up your and GIT's list of branches with dead branches. It ensures you only ever merge the branch once. Dev branches should only exist while the work is still in progress.

### Coding Style

    It is advisable to run code analysis from your IDE and ensure no errors exists.
    IDE also suggest best practices to write code and can help standardize your codebase. 

### Component Naming

    Refer Google Java Style Guide: 
    https://google.github.io/styleguide/javaguide.html

### Directory Casing

    DO use lowercase only or camel cases for directory name.
