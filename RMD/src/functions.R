dt_lingokids <- function(dt, ndec = 2,width = NULL){
  # dt %>%  datatable(options = list(dom = 't',autoWidth = TRUE), rownames= FALSE, width = width,fillContainer =T)
  dt_k <- dt %>%  
    mutate_if(is.numeric, funs(round(.,ndec))) %>% 
    kable("html") %>%
    kable_styling(full_width = F,bootstrap_options = c("striped", "hover", "condensed", "responsive"))
  if(!is.null(width)){
    dt_k <- dt_k %>%
      scroll_box(width = width)
  }
  dt_k
}

theme_lingokids <- function(){
  windowsFonts(Gotham=windowsFont("Gotham Rounded Book"))
  
  theme_minimal()  +
    theme(panel.grid.minor = element_line(colour = NA),
          text=element_text(family = "Gotham"))
}
