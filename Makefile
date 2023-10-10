tidy:
	go clean -modcache
	rm -Rf go.sum
	go env -w GOPRIVATE=suntech.com.vn/skylib/skylog.git,suntech.com.vn/skylib/skyutl.git
	go mod tidy

run:
	go run src/main.go

tags:
	git ls-remote --tags


commit:
	git status
	git add .
	git commit -m"$m"
	git push
	git tag $t
	git push --tags