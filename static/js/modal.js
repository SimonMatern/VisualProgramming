var BstrapModal = function (title, body, buttons) {
            var title = title || "Lorem Ipsum History",
                body = body || "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged.",
                buttons = buttons || [{
                    Value: "CLOSE", Css: "btn-primary", Callback: function (event) {
                        BstrapModal.Close();
                    }
                }];
            var GetModalStructure = function () {
                var that = this;
                that.Id = BstrapModal.Id = Math.random();
                var buttonshtml = "";
                for (var i = 0; i < buttons.length; i++) {
                    buttonshtml += "<button type='button' class='btn " + (buttons[i].Css || "") + "' name='btn" + that.Id + "'>"
                        + (buttons[i].Value || "CLOSE") + "</button>";
                }
                return`<div class="modal fade" name='dynamiccustommodal' id=${that.Id} tabindex="-1" role="dialog" aria-labelledby="staticBackdropLabel" aria-hidden="true">
                          <div class="modal-dialog" role="document">
                            <div class="modal-content">
                              <div class="modal-header">
                                <h5 class="modal-title" id="staticBackdropLabel">${title}</h5>
                                <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                                  <span aria-hidden="true">&times;</span>
                                </button>
                              </div>
                              <div class="modal-body">
                                ${body}
                              </div>
                              <div class="modal-footer">
                              ${buttonshtml}
                              </div>
                            </div>
                          </div>
                        </div>`;
            }();
            BstrapModal.Delete = function () {
                var modals = document.getElementsByName("dynamiccustommodal");
                if (modals.length > 0) document.body.removeChild(modals[0]);
            };
            BstrapModal.Close = function () {
                $(document.getElementById(BstrapModal.Id)).modal('hide');
                BstrapModal.Delete();
            };
            this.Show = function () {
                BstrapModal.Delete();
                document.body.appendChild($(GetModalStructure)[0]);
                var btns = document.querySelectorAll("button[name='btn" + BstrapModal.Id + "']");
                for (var i = 0; i < btns.length; i++) {
                    btns[i].addEventListener("click", buttons[i].Callback || BstrapModal.Close);
                }
                $(document.getElementById(BstrapModal.Id)).modal('show');
            };
        };