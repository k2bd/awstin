# Making a Release

To release, open a PR to master. Normal merge, do not squash the commit.

Once merged to master, a new version should be automatically built and uploaded
to PyPI.

Then open a PR to develop that bumps the version in `awstin/__init__.py`. This
is a good opportunity to re-enable squash and merge.
