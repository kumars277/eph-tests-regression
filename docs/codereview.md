# Code Review Guidelines:
Peer code reviews are the single biggest thing you can do to improve your code.

### Code Review Etiquette

Everyone in the peer review process is expected to behave in a professional manner.

    1. Be respectful at all time
    2. Don't be defensive
    3. Be open to different approaches
    4. Provide constructive feedback
    5. Facts vs opinions
    6. Refer to documentation
    7. Don't cause unnecessary delays

These are considered "social faux pas":

    1. Creating tasks on a pull request that isn't yours without checking with the author first
    2. Merging a pull request that isn't yours without checking with the author first
    3. Declining a pull request that isn't yours without checking with the author first

### Reviewer/Author Checklist

    1. Does the pull request meet every requirements?
    2. Does the pull request meet our Non-Functional Requirements?
    3. Does the pull request come with enough appropriate testing?
    4. Does the pull request touch a part that is shared with or would impact another team?


### Reviewer Checklist

    1. Is the code easy to understand?
    2. Is there anything unexpected?
    3. Is there any "code smell"?
    4. Is there any potential bug or edge case?
    5. Is there any opportunity for code reuse or optimisation?
    6. Is there any false positive test?

### Author Checklist
    
    1. Is the linter happy?
    2. Do all tests pass?
    3. Is Jenkins green (or blue)?
    4. Is the documentation up to date?

### Guidelines For Reviewers

    1. Let the author know when you start and finish reviewing with a comment
    2. If the linter doesn't complain nor should you ;)
    3. Make reasonable efforts to understand the code and ask if something is not clear
    4. Make suggestions (not demands) and explain why
    5. Make reasonable requests and/or provide alternative solutions
    6. Label non-blocking comments e.g. nit, minor, subjective

### Guidelines For Authors

    1. Avoid large pull requests
    2. Big architectural changes should be discussed beforehand
    3. Focus on readable code instead of "clever" code
    4. Let people know when you need code review
    5. Add comments where appropriate i.e. comments to explain the PR or comments when major updates are made to the PR
    6. Follow up! No stale pull requests
    7. Address and/or reply to all comments before you merge
