#import "page.typ": page
#import "meta.typ": site
#show: page.with(body-class: "home")

#html.h1()[#site.name]
#html.p(class: "lede")[An experimental Kubernetes gateway built in Rust with io_uring.]
