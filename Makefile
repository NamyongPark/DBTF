all: build install demo

demo:
	./demo-cp.sh && ./demo-tk.sh
    
demo-cp:
	./demo-cp.sh

demo-tk:
	./demo-tk.sh

install:
	chmod +x *.sh

build:
	sbt assembly