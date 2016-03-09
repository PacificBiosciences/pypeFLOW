SHELL:=bash
run:
	pwatcher-main run < run.json
	pstree -pgsu $${USER}
query:
	pwatcher-main query < query-ab.json
delete:
	pwatcher-main delete <<< $$(echo '{"which":"infer"}')
clean:
	rm -rf pwatched *.log
